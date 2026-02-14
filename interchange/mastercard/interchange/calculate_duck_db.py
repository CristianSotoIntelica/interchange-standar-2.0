import pandas as pd
import duckdb
from pathlib import Path
from interchange.persistence.database import Database
from interchange.logs.logger import Logger
import re

log = Logger(__name__)


def calculate_pre_eval(
    *,
    parquet_txn_path: str | Path,
    parquet_calc_path: str | Path,
    db: Database,
    brand_fx_eval: str = "MASTERCARD",        
    client_id: str | None = None,
    file_id: str | None = None,
) -> pd.DataFrame:
  
    """
    - Une parquet transaccional + calculated
    - Mapea currency numeric -> alpha
    - Lee monedas objetivo desde mc_rules.amount_transaction_currency
    - Genera columnas dinámicas amount_transaction_{CCY}
      usando exchange_rate (por fecha y from->to)

    Output: DF "evaluate" listo para mastercard_interchange_rule_assign()
    """

    p_txn = str(Path(parquet_txn_path).resolve())
    p_cal = str(Path(parquet_calc_path).resolve())

    # 1) Monedas objetivo desde reglas
    df_targets = db.read_sql("""
        SELECT amount_transaction_currency
        FROM mc_rules
        WHERE amount_transaction_currency IS NOT NULL
        GROUP BY 1
    """)
    target_ccys = [str(x).strip() for x in df_targets["amount_transaction_currency"].tolist() if str(x).strip()]

    # 2) Currency dim
    df_curr = db.read_sql("""
        SELECT currency_numeric_code, currency_alphabetic_code
        FROM currency
    """)

    # 3) Exchange rates (para construir amount_transaction_{CCY})
    # en legacy se armaban LEFT JOIN por cada moneda objetivo
    df_ex = db.read_sql("""
        SELECT
            rate_date,          -- o app_processing_date
            brand,
            currency_from_code, -- o currency_from (si es alpha)
            currency_to,
            exchange_value
        FROM exchange_rate
        WHERE upper(brand)=upper(?)
    """, params=(brand_fx_eval,))

    con = duckdb.connect()
    try:
        con.register("cur", df_curr)
        con.register("ex", df_ex)

        # arma dinámicamente select de monedas objetivo (como str_currency del legacy)
        dyn_cols = []
        dyn_joins = []
        for ccy in target_ccys:
            alias = f"ex_{ccy}"
            # columna: amount_transaction_{ccy}
            dyn_cols.append(
                f""",
                COALESCE(
                    CAST(
                        (t.amount_transaction *
                            CASE
                                WHEN upper(cur.currency_alphabetic_code) = upper('{ccy}') THEN 1
                                ELSE {alias}.exchange_value
                            END
                        ) AS VARCHAR
                    ),
                    'BLANK'
                ) AS amount_transaction_{ccy}
                """
            )
            # join FX por fecha + from + to
            # Ajusta columnas de ex (rate_date / currency_from_code) según tu esquema real
            dyn_joins.append(
                f"""
                LEFT JOIN ex {alias}
                  ON CAST({alias}.rate_date AS DATE) = CAST(strftime(t.date_and_time_local_transaction, '%Y-%m-%d') AS DATE)
                 AND try_cast({alias}.currency_from_code AS BIGINT) = try_cast(t.currency_code_transaction AS BIGINT)
                 AND upper({alias}.currency_to) = upper('{ccy}')
                """
            )

        dyn_cols_sql = "\n".join(dyn_cols)
        dyn_joins_sql = "\n".join(dyn_joins)

        sql = f"""
        WITH txn AS (
            SELECT
                file_id,
                ref_id,
                pan_de_2 AS pan,                         -- para issuer_bin_8
                acquirer_reference_data_de_31 AS acquirer_reference_data,    
                --electronic_commerce_indicator_3,
                processing_code_de_3 AS processing_code,
                "card_acceptor_business_code_[mcc]_de_26" AS card_acceptor_business_code,
                date_and_time_local_transaction_de_12 AS date_and_time_local_transaction,
                business_activity_4_pds_158_4 AS ird, 
                amount_transaction_de_4 as amount_transaction,
                currency_code_transaction_de_49 as currency_code_transaction,
                mastercard_assigned_id_pds_176 as mastercard_assigned_id        -- si lo usas en reglas
            FROM read_parquet(?)
        ),
        calc AS (
            SELECT
                file_id,
                ref_id,
                jurisdiction_assigned AS jurisdiction,
                gcms_product_identifier,      -- si lo usas en reglas
                funding_source,      -- si lo usas en reglas
                settlement_report_amount,
                settlement_report_currency_code
            FROM read_parquet(?)
        )
        SELECT
            -- ids
            t.file_id,
            t.ref_id,

            -- campos para reglas (legacy)
            COALESCE(TRIM(SUBSTR(CAST(t.pan AS VARCHAR), 1, 8)), 'BLANK') AS issuer_bin_8,
            COALESCE(TRIM(SUBSTR(CAST(t.acquirer_reference_data AS VARCHAR), 2, 6)), 'BLANK') AS acquirer_bin,
            --COALESCE(TRIM(CAST(t.electronic_commerce_indicator_3 AS VARCHAR)), 'BLANK') AS electronic_commerce_indicator_3,
            
            COALESCE(TRIM(CAST(ca.jurisdiction AS VARCHAR)), 'BLANK') AS jurisdiction,
            COALESCE(TRIM(CAST(t.ird AS VARCHAR)), 'BLANK') AS ird,

            COALESCE(TRIM(SUBSTR(CAST(t.processing_code AS VARCHAR), 1, 2)), 'BLANK') AS processing_code,
            COALESCE(CAST(t.amount_transaction AS VARCHAR), 'BLANK') AS amount_transaction,
            COALESCE(CAST(settlement_report_amount AS VARCHAR), 'BLANK') settlement_report_amount,
            COALESCE(CAST(settlement_report_currency_code AS VARCHAR), 'BLANK') settlement_report_currency_code ,
            COALESCE(CAST(cur.currency_alphabetic_code AS VARCHAR), 'BLANK') AS amount_transaction_currency,

            COALESCE(TRIM(CAST(t.card_acceptor_business_code AS VARCHAR)), 'BLANK') AS card_acceptor_business_code,

            -- opcionales para reglas
            COALESCE(TRIM(CAST(ca.gcms_product_identifier AS VARCHAR)), 'BLANK') AS gcms_product_identifier,
            COALESCE(TRIM(CAST(ca.funding_source AS VARCHAR)), 'BLANK') AS funding_source,
            COALESCE(CAST(t.mastercard_assigned_id AS VARCHAR), 'BLANK') AS mastercard_assigned_id,

            -- para vigencia / FX posterior
            CAST(strftime(t.date_and_time_local_transaction, '%Y-%m-%d') AS DATE) AS txn_date,
            try_cast(t.currency_code_transaction AS BIGINT) AS currency_code_transaction

            {dyn_cols_sql}

        FROM txn t
        INNER JOIN calc ca
          ON t.file_id = ca.file_id
         AND t.ref_id  = ca.ref_id

        LEFT JOIN cur
          ON try_cast(cur.currency_numeric_code AS BIGINT) = try_cast(t.currency_code_transaction AS BIGINT)

        {dyn_joins_sql}
        """

        df_eval = con.execute(sql, [p_txn, p_cal]).df()
        log.logger.debug(f"[calculate_pre_eval_full_legacy] rows={len(df_eval)} targets={len(target_ccys)}")
        return df_eval

    finally:
        con.close()

RANGE_COLS = {"issuer_bin_8", "acquirer_bin", "card_acceptor_business_code"}

def _blanklike(v: object) -> bool:
    s = str(v).strip()
    return s == "" or s.lower() in {"none", "nan", "null"}

def _split_csv_nospace(s: str) -> list[str]:
    return [x for x in str(s).replace(" ", "").split(",") if x != ""]

def _is_range_token(tok: str) -> bool:
    tok = str(tok).strip()
    if "-" not in tok:
        return False
    a, b = tok.split("-", 1)
    return a.strip().isdigit() and b.strip().isdigit()

def _parse_list_and_ranges(expr: str):
    """
    "123,124,200-210" -> (["123","124"], [(200,210)])
    """
    expr = str(expr).strip()
    if not expr:
        return [], []
    parts = _split_csv_nospace(expr)
    vals, rngs = [], []
    for p in parts:
        if _is_range_token(p):
            a, b = p.split("-", 1)
            rngs.append((int(a), int(b)))
        else:
            vals.append(p)
    return vals, rngs

def _parse_amount_legacy(expr: str):
    """
    Legacy:
      - comparadores: ">=10,<=20"
      - between: "between10and20" (sin espacios)
      - else igualdad (texto exacto)
    Retorna lista:
      ("between", lo, hi) | ("cmp", op, val) | ("eq", text)
    """
    e = str(expr).strip()
    if _blanklike(e):
        return []
    e0 = e.replace(" ", "")
    el = e0.lower()

    if "between" in el:
        tmp = el.replace("between", "")
        if "and" in tmp:
            a, b = tmp.split("and", 1)
            try:
                return [("between", float(a), float(b))]
            except Exception:
                return []

    if any(op in e0 for op in ["<", ">", "="]):
        out = []
        for part in e0.split(","):
            part = part.strip()
            if not part:
                continue
            m = re.match(r"^(<=|>=|=|<|>)(-?\d+(\.\d+)?)$", part)
            if m:
                out.append(("cmp", m.group(1), float(m.group(2))))
        return out

    return [("eq", e0.strip())]


def assign_rules(
    *,
    df_eval: pd.DataFrame,
    db,
    extras: list[str] | None = None,
) -> pd.DataFrame:
    """
   
      1) DuckDB retorna SOLO lo necesario:
         id, rule(key), region_country_code, intelica_id, ird, rate_*, valid_*
      2) Luego hacemos merge con df_eval para traer columnas extra (si existen),
         sin depender del SELECT grande.

    Semántica legacy preservada:
      - first-match-wins por key (prioridad: region_country_code, intelica_id num)
      - soporta NOT:, listas, y rangos en RANGE_COLS (sin expandir rangos)
      - amount_transaction usa amount_transaction_{ccy} si existe en df_eval
      - congela la versión vigente por txn_date (valid_from/valid_until)

    Requiere df_eval con:
      file_id, ref_id, jurisdiction, ird, txn_date
    """

    if df_eval is None or df_eval.empty:
        return pd.DataFrame(columns=[
            "file_id","ref_id","rule","region_country_code","intelica_id","ird",
            "rate_currency","rate_variable","rate_fixed","rate_min","rate_cap",
            "valid_from","valid_until",
        ])

    # ---- Work ----
    work = df_eval.copy()
    work["id"] = range(1, len(work) + 1)

    required = {"file_id", "ref_id", "jurisdiction", "ird", "txn_date"}
    missing_req = [c for c in required if c not in work.columns]
    if missing_req:
        raise ValueError(f"df_eval debe incluir {sorted(required)}. Faltan: {missing_req}")

    # ---- Load rules ----
    rules_raw = pd.DataFrame(db.read_sql("SELECT * FROM mc_rules"))
    if rules_raw.empty:
        out = work[["id","file_id","ref_id"]].copy()
        out["rule"] = 0
        out["region_country_code"] = None
        out["intelica_id"] = None
        out["ird"] = work["ird"].astype(str)
        out["rate_currency"] = None
        out["rate_variable"] = None
        out["rate_fixed"] = None
        out["rate_min"] = None
        out["rate_cap"] = None
        out["valid_from"] = pd.NaT
        out["valid_until"] = pd.NaT
        # merge extras al final
        return _merge_extras(out, work, extras)

    # prioridad legacy => key
    rules = rules_raw.copy()
    rules["_intelica_num"] = pd.to_numeric(rules.get("intelica_id"), errors="coerce")
    rules = rules.sort_values(["region_country_code","_intelica_num"], na_position="last").reset_index(drop=True)
    rules["key"] = range(1, len(rules) + 1)

    excluded = {
        "app_creation_user","app_creation_date","key",
        "amount_transaction_currency","jurisdiction","region_country_code","guide_date",
        "valid_from","valid_until","fee_category","fee_tier","intelica_id","ird",
        "rate_currency","rate_variable","rate_fixed","rate_min","rate_cap",
        "masterpass_incentive_indicator","tti","additional_data",
        "_intelica_num"
    }
    rule_cols = [c for c in rules.columns if c not in excluded]

    # ---- Normalize rule conditions into tables ----
    pos_vals, neg_vals = [], []   # (key,col,val)
    pos_rng,  neg_rng  = [], []   # (key,col,lo,hi)
    amt_rules = []                # (key,ccy,kind,op,lo,hi,eq_str)

    for _, rr in rules.iterrows():
        k = int(rr["key"])

        # amount_transaction
        amt_expr = rr.get("amount_transaction", None)
        if amt_expr is not None and not _blanklike(amt_expr):
            ccy = str(rr.get("amount_transaction_currency", "")).strip().lower()
            preds = _parse_amount_legacy(str(amt_expr))
            for p in preds:
                if p[0] == "between":
                    _, lo, hi = p
                    amt_rules.append((k, ccy, "between", None, float(lo), float(hi), None))
                elif p[0] == "cmp":
                    _, op, val = p
                    amt_rules.append((k, ccy, "cmp", op, float(val), None, None))
                else:
                    _, eqs = p
                    amt_rules.append((k, ccy, "eq", None, None, None, str(eqs).strip()))

        for col in rule_cols:
            if col == "amount_transaction":
                continue

            v = rr.get(col, None)
            if v is None or _blanklike(v):
                continue

            s = str(v).replace(" ", "")
            is_not = s.startswith("NOT:")
            if is_not:
                s = s.replace("NOT:", "").strip()

            vals, rngs = _parse_list_and_ranges(s)

            for val in vals:
                rec = (k, col, str(val).strip().upper())
                (neg_vals if is_not else pos_vals).append(rec)

            if col in RANGE_COLS:
                for lo, hi in rngs:
                    rec2 = (k, col, int(lo), int(hi))
                    (neg_rng if is_not else pos_rng).append(rec2)
            else:
                # rango en col no-range -> literal
                for lo, hi in rngs:
                    rec3 = (k, col, f"{lo}-{hi}".upper())
                    (neg_vals if is_not else pos_vals).append(rec3)

    df_pos_vals = pd.DataFrame(pos_vals, columns=["key","col","val"]) if pos_vals else pd.DataFrame(columns=["key","col","val"])
    df_neg_vals = pd.DataFrame(neg_vals, columns=["key","col","val"]) if neg_vals else pd.DataFrame(columns=["key","col","val"])
    df_pos_rng  = pd.DataFrame(pos_rng,  columns=["key","col","lo","hi"]) if pos_rng else pd.DataFrame(columns=["key","col","lo","hi"])
    df_neg_rng  = pd.DataFrame(neg_rng,  columns=["key","col","lo","hi"]) if neg_rng else pd.DataFrame(columns=["key","col","lo","hi"])
    df_amt      = pd.DataFrame(amt_rules, columns=["key","ccy","kind","op","lo","hi","eq_str"]) if amt_rules else pd.DataFrame(columns=["key","ccy","kind","op","lo","hi","eq_str"])

    # columns used by values that exist in work
    cond_cols = sorted(set(df_pos_vals["col"].unique()).union(set(df_neg_vals["col"].unique())))
    cond_cols = [c for c in cond_cols if c in work.columns]

    # range cols present
    range_cols_present = [c for c in RANGE_COLS if c in work.columns]

    # ---- amount SQL block (optional) ----
    amt_sql = ""
    if not df_amt.empty:
        ccys = sorted({str(x).strip().lower() for x in df_amt["ccy"].tolist() if str(x).strip()})
        case_lines = []
        for ccy in ccys:
            colname = f"amount_transaction_{ccy}"
            if colname in work.columns:
                case_lines.append(f"WHEN a.ccy = '{ccy}' THEN try_cast(w2.\"{colname}\" as double)")
        if case_lines:
            amt_val_expr = "CASE " + " ".join(case_lines) + " ELSE NULL END"
            amt_sql = f"""
            AND (
              (select count(*) from amt a0 where a0.key=b.key) = 0
              OR (
                select bool_and(
                  case
                    when a.kind='between' then {amt_val_expr} between a.lo and a.hi
                    when a.kind='cmp' then
                      case a.op
                        when '>=' then {amt_val_expr} >= a.lo
                        when '<=' then {amt_val_expr} <= a.lo
                        when '>'  then {amt_val_expr} >  a.lo
                        when '<'  then {amt_val_expr} <  a.lo
                        when '='  then {amt_val_expr} =  a.lo
                        else true
                      end
                    when a.kind='eq' then cast({amt_val_expr} as varchar) = a.eq_str
                    else true
                  end
                )
                from amt a
                where a.key=b.key
              )
            )
            """
        else:
            amt_sql = "AND ((select count(*) from amt a0 where a0.key=b.key) = 0)"

    # ---- CASE for dynamic column selection ----
    def _case_value(ref_col: str) -> str:
        lines = [f"WHEN {ref_col} = '{c}' THEN w2.\"{c}_u\"" for c in cond_cols]
        return "CASE " + " ".join(lines) + " ELSE NULL END"

    def _case_range(ref_col: str) -> str:
        lines = [f"WHEN {ref_col} = '{c}' THEN w2.\"{c}_num\"" for c in range_cols_present]
        return "CASE " + " ".join(lines) + " ELSE NULL END"

    case_pos_val = _case_value("pv.col")
    case_neg_val = _case_value("nv.col")
    case_pos_rng = _case_range("pr.col")
    case_neg_rng = _case_range("nr.col")

    # ---- SQL: return MINIMAL result with id ----
    # IMPORTANT: commas are injected correctly (we add leading commas ourselves)
    extra_u = "".join([f', upper(trim(cast(w."{c}" as varchar))) as "{c}_u"' for c in cond_cols])
    extra_num = "".join([f', try_cast(w."{c}" as BIGINT) as "{c}_num"' for c in range_cols_present])

    sql = f"""
WITH
work2 AS (
  SELECT
    w.*,
    try_cast(w.txn_date as DATE) AS txn_date_d,
    upper(trim(cast(w.jurisdiction as varchar))) AS jurisdiction_u,
    upper(trim(cast(w.ird as varchar))) AS ird_u
    {extra_u}
    {extra_num}
  FROM work w
),
base AS (
  SELECT
    w2.id, w2.file_id, w2.ref_id, w2.txn_date_d,
    r.key, r.region_country_code, r.intelica_id,
    upper(trim(cast(r.region_country_code as varchar))) AS region_country_code_u,
    upper(trim(cast(r.ird as varchar))) AS ird_rule_u
  FROM work2 w2
  JOIN rules r
    ON w2.jurisdiction_u = upper(trim(cast(r.region_country_code as varchar)))
   AND w2.ird_u          = upper(trim(cast(r.ird as varchar)))
),
best_rule AS (
  SELECT *
  FROM (
    SELECT
      b.id, b.file_id, b.ref_id, b.txn_date_d,
      b.key,
      b.region_country_code_u,
      b.ird_rule_u AS ird_u,
      try_cast(b.intelica_id as BIGINT) AS intelica_id_num,
      row_number() over (partition by b.id order by b.key) as rn
    FROM base b
    JOIN work2 w2 ON w2.id = b.id
    WHERE 1=1

      AND NOT EXISTS (
        SELECT 1
        FROM (SELECT DISTINCT key, col FROM pos_vals) req
        WHERE req.key = b.key
          AND NOT EXISTS (
            SELECT 1
            FROM pos_vals pv
            WHERE pv.key = b.key
              AND pv.col = req.col
              AND {case_pos_val} = pv.val
          )
      )

      AND NOT EXISTS (
        SELECT 1
        FROM neg_vals nv
        WHERE nv.key = b.key
          AND {case_neg_val} = nv.val
      )

      AND NOT EXISTS (
        SELECT 1
        FROM (SELECT DISTINCT key, col FROM pos_rng) reqr
        WHERE reqr.key = b.key
          AND NOT EXISTS (
            SELECT 1
            FROM pos_rng pr
            WHERE pr.key = b.key
              AND pr.col = reqr.col
              AND {case_pos_rng} BETWEEN pr.lo AND pr.hi
          )
      )

      AND NOT EXISTS (
        SELECT 1
        FROM neg_rng nr
        WHERE nr.key = b.key
          AND {case_neg_rng} BETWEEN nr.lo AND nr.hi
      )

      {amt_sql}

  ) x
  WHERE rn = 1
),
rver AS (
  SELECT
    key,
    try_cast(intelica_id as BIGINT) AS intelica_id_num,
    upper(trim(cast(region_country_code as varchar))) AS region_country_code_u,
    upper(trim(cast(ird as varchar))) AS ird_u,
    cast(valid_from as DATE) AS valid_from_d,
    cast(valid_until as DATE) AS valid_until_d,
    rate_currency,
    try_cast(rate_variable as DOUBLE) AS rate_variable,
    try_cast(rate_fixed as DOUBLE)    AS rate_fixed,
    try_cast(rate_min as DOUBLE)      AS rate_min,
    try_cast(rate_cap as DOUBLE)      AS rate_cap
  FROM rules
),
best_with_rates AS (
  SELECT
    br.*,
    rv.rate_currency,
    rv.rate_variable,
    rv.rate_fixed,
    rv.rate_min,
    rv.rate_cap,
    rv.valid_from_d,
    rv.valid_until_d,
    row_number() over (partition by br.id order by rv.valid_from_d desc nulls last) as rn2
  FROM best_rule br
  LEFT JOIN rver rv
    ON rv.intelica_id_num = br.intelica_id_num
   AND rv.region_country_code_u = br.region_country_code_u
   AND rv.ird_u = br.ird_u
   AND rv.valid_from_d <= br.txn_date_d
   AND (rv.valid_until_d IS NULL OR rv.valid_until_d >= br.txn_date_d)
)
SELECT
  w2.id,
  w2.file_id,
  w2.ref_id,
  coalesce(bwr.key, 0) AS rule,
  bwr.region_country_code_u AS region_country_code,
  cast(bwr.intelica_id_num as varchar) AS intelica_id,
  coalesce(bwr.ird_u, upper(trim(cast(w2.ird as varchar)))) AS ird,
  bwr.rate_currency,
  bwr.rate_variable,
  bwr.rate_fixed,
  bwr.rate_min,
  bwr.rate_cap,
  bwr.valid_from_d AS valid_from,
  bwr.valid_until_d AS valid_until
FROM work2 w2
LEFT JOIN (SELECT * FROM best_with_rates WHERE rn2 = 1) bwr
  ON bwr.id = w2.id
"""

    con = duckdb.connect()
    try:
        con.register("work", work)
        con.register("rules", rules)
        con.register("pos_vals", df_pos_vals)
        con.register("neg_vals", df_neg_vals)
        con.register("pos_rng", df_pos_rng)
        con.register("neg_rng", df_neg_rng)
        con.register("amt", df_amt)

        out_min = con.execute(sql).df()
    finally:
        con.close()

    # ---- Merge extras robustly ----
    return _merge_extras(out_min, work, extras)


def _merge_extras(out_min: pd.DataFrame, work: pd.DataFrame, extras: list[str] | None) -> pd.DataFrame:
    """
    Trae columnas extra desde work sin depender del SQL.
    """
    # extras default (tú puedes pasar tu lista)
    default_extras = [
        "jurisdiction",
        "processing_code",
        "card_acceptor_business_code",
        "amount_transaction",
        "amount_transaction_currency",
        "settlement_report_amount",
        "settlement_report_currency_code",
        "txn_date",
    ]
    extras = default_extras if extras is None else extras

    # mantener solo las que existen
    extras_present = [c for c in extras if c in work.columns]

    if extras_present:
        out = out_min.merge(
            work[["id"] + extras_present],
            on="id",
            how="left",
        )
    else:
        out = out_min.copy()

    # id ya no lo necesitas afuera
    if "id" in out.columns:
        out = out.drop(columns=["id"])

    return out

def calculate_mastercard_fee(
    *,
    df_assign: pd.DataFrame,
    db: Database,
    brand_fx_eval: str = "MASTERCARD",
) -> pd.DataFrame:

    df_ex = db.read_sql("""
        SELECT
            rate_date,
            brand,
            currency_from,
            currency_from_code,
            currency_to,
            exchange_value
        FROM exchange_rate
        WHERE upper(brand)=upper(?)
    """, params=(brand_fx_eval,))

    con = duckdb.connect()
    try:
        con.register("a", df_assign)
        con.register("ex", df_ex)

        sql = f"""
        WITH a2 AS (
          SELECT
            a.*,
            try_cast(a.amount_transaction AS DOUBLE) AS amount_transaction_num,
            try_cast(a.rate_variable AS DOUBLE)      AS rate_variable_num,
            try_cast(a.rate_fixed AS DOUBLE)         AS rate_fixed_num,
            try_cast(a.rate_min AS DOUBLE)           AS rate_min_num,
            try_cast(a.rate_cap AS DOUBLE)           AS rate_cap_num,
            try_cast(a.txn_date AS DATE)             AS txn_date_d
          FROM a
        ),
        ex2 AS (
          SELECT
            try_cast(ex.rate_date AS DATE) AS rate_date_d,
            upper(ex.brand) AS brand_u,
            upper(ex.currency_from) AS currency_from_u,
            upper(ex.currency_to) AS currency_to_u,
            try_cast(ex.exchange_value AS DOUBLE) AS exchange_value_num
          FROM ex
        )
        SELECT
            a2.*,

            -- =========================
            -- FX multiplier
            -- =========================
            CASE
                WHEN a2.rate_currency IS NULL THEN 1.0
                WHEN upper(a2.rate_currency) = upper(a2.amount_transaction_currency) THEN 1.0
                ELSE ex2.exchange_value_num
            END AS fx_multiplier,

            -- =========================
            -- Amount convertido
            -- =========================
            a2.amount_transaction_num *
            CASE
                WHEN a2.rate_currency IS NULL THEN 1.0
                WHEN upper(a2.rate_currency) = upper(a2.amount_transaction_currency) THEN 1.0
                ELSE ex2.exchange_value_num
            END AS amount_converted,

            -- =========================
            -- Fee preliminar
            -- =========================
            (
                coalesce(a2.rate_variable_num,0.0) *
                (
                    a2.amount_transaction_num *
                    CASE
                        WHEN a2.rate_currency IS NULL THEN 1.0
                        WHEN upper(a2.rate_currency) = upper(a2.amount_transaction_currency) THEN 1.0
                        ELSE ex2.exchange_value_num
                    END
                )
            )
            + coalesce(a2.rate_fixed_num,0.0) AS fee_preliminary,

            -- =========================
            -- Aplicar min / cap
            -- =========================
            CASE
                WHEN a2.rate_variable IS NULL
                    THEN coalesce(a2.rate_fixed_num,0.0)

                WHEN a2.rate_variable_num IS NULL THEN NULL
                WHEN a2.amount_transaction_num IS NULL THEN NULL

                -- si se necesitaba FX y no existe, devuelve NULL (para no inventar fee)
                WHEN a2.rate_currency IS NOT NULL
                 AND upper(a2.rate_currency) <> upper(a2.amount_transaction_currency)
                 AND ex2.exchange_value_num IS NULL
                THEN NULL

                ELSE
                    LEAST(
                        coalesce(a2.rate_cap_num, 1e18),
                        GREATEST(
                            coalesce(a2.rate_min_num, -1e18),
                            (
                                coalesce(a2.rate_variable_num,0.0) *
                                (
                                    a2.amount_transaction_num *
                                    CASE
                                        WHEN a2.rate_currency IS NULL THEN 1.0
                                        WHEN upper(a2.rate_currency) = upper(a2.amount_transaction_currency) THEN 1.0
                                        ELSE ex2.exchange_value_num
                                    END
                                )
                            )
                            + coalesce(a2.rate_fixed_num,0.0)
                        )
                    )
            END AS calculated_fee

        FROM a2
        LEFT JOIN ex2
          ON ex2.rate_date_d = a2.txn_date_d
         AND ex2.currency_from_u = upper(a2.amount_transaction_currency)
         AND ex2.currency_to_u = upper(a2.rate_currency)
         AND ex2.brand_u = upper('{brand_fx_eval}')
        """

        return con.execute(sql).df()

    finally:
        con.close()