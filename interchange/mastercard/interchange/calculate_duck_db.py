import pandas as pd
import duckdb
from pathlib import Path
from interchange.persistence.database import Database
from interchange.logs.logger import Logger
from typing import Optional
import re

# Logger estándar del proyecto (se usa para debug de filas/targets, etc.)
log = Logger(__name__)

def calculate_pre_eval(
    *,
    parquet_txn_path: str | Path,
    parquet_calc_path: str | Path,
    db: Database,
    brand_fx_eval: str = "MASTERCARD",
    client_id: str | None = None,
    file_id: str | None = None,
    **kwargs,
) -> pd.DataFrame:

    p_txn = str(Path(parquet_txn_path).resolve())
    p_cal = str(Path(parquet_calc_path).resolve())

    # =============================
    # TARGET CURRENCIES (rules)
    # =============================
    df_targets = db.read_sql("""
        SELECT upper(trim(amount_transaction_currency)) AS ccy
        FROM mc_rules
        WHERE amount_transaction IS NOT NULL
          AND trim(cast(amount_transaction as varchar)) <> ''
          AND amount_transaction_currency IS NOT NULL
          AND trim(cast(amount_transaction_currency as varchar)) <> ''
        GROUP BY 1
    """)

    if df_targets is None or df_targets.shape[1] == 0:
        df_targets = pd.DataFrame(columns=["ccy"])

    target_ccys = (
        df_targets["ccy"]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    # =============================
    # CURRENCY TABLE
    # =============================
    df_curr = db.read_sql("""
        SELECT currency_numeric_code, currency_alphabetic_code
        FROM currency
    """)

    if df_curr is None or df_curr.shape[1] == 0:
        df_curr = pd.DataFrame(
            columns=["currency_numeric_code","currency_alphabetic_code"]
        )

    # =============================
    # FX TABLE (SQLITE SAFE)
    # =============================
    try:
        df_ex = db.read_sql("""
            SELECT
                rate_date,
                brand,
                currency_from_code,
                currency_to,
                exchange_value
            FROM exchange_rate
            WHERE upper(brand)=upper(?)
        """, params=(brand_fx_eval,))
    except Exception:
        df_ex = pd.DataFrame(
            columns=[
                "rate_date",
                "brand",
                "currency_from_code",
                "currency_to",
                "exchange_value",
            ]
        )

    if df_ex is None or df_ex.shape[1] == 0:
        df_ex = pd.DataFrame(
            columns=[
                "rate_date",
                "brand",
                "currency_from_code",
                "currency_to",
                "exchange_value",
            ]
        )

    # =============================
    # DYNAMIC FX PIVOT
    # =============================
    pivot_exprs = []
    for ccy in target_ccys:
        ccy_u = ccy.upper().strip()
        ccy_l = ccy_u.lower()

        pivot_exprs.append(
            f"""
            max(
              case
                when currency_to_u='{ccy_u}'
                then exchange_value_num
              end
            ) as fx_to_{ccy_l}
            """
        )

    pivot_sql = ",\n".join(pivot_exprs) if pivot_exprs else "NULL as fx_dummy"

    # =============================
    # DYNAMIC amount_transaction_ccy
    # =============================
    dyn_cols = []
    for ccy in target_ccys:
        ccy_u = ccy.upper().strip()
        ccy_l = ccy_u.lower()

        dyn_cols.append(f"""
        , coalesce(
            cast(
                (
                    try_cast(w2.amount_transaction as double) *
                    case
                        when upper(trim(cast(w2.trx_ccy_alpha as varchar)))='{ccy_u}'
                        then 1.0
                        else ex_p.fx_to_{ccy_l}
                    end
                ) as varchar
            ),
            'BLANK'
        ) as amount_transaction_{ccy_l}
        """)

    dyn_cols_sql = "\n".join(dyn_cols)

    # =============================
    # MAIN SQL (DUCKDB)
    # =============================
    sql = f"""
    WITH

    txn AS (
        SELECT
            file_id,
            ref_id,
            file_idn,
            pan_de_2 AS pan,
            acquirer_reference_data_de_31 AS acquirer_reference_data,
            processing_code_de_3 AS processing_code,
            "card_acceptor_business_code_[mcc]_de_26" AS card_acceptor_business_code,
            date_and_time_local_transaction_de_12 AS date_and_time_local_transaction,
            business_activity_4_pds_158_4 AS ird,
            amount_transaction_de_4 AS amount_transaction,
            currency_code_transaction_de_49 AS currency_code_transaction,
            mastercard_assigned_id_pds_176 AS mastercard_assigned_id
        FROM read_parquet(?)
    ),

    calc AS (
        SELECT
            file_id,
            ref_id,
            file_idn,
            jurisdiction_assigned AS jurisdiction,
            gcms_product_identifier,
            funding_source,
            settlement_report_amount,
            settlement_report_currency_code
        FROM read_parquet(?)
    ),

    work AS (
        SELECT
            t.*,
            ca.jurisdiction,
            ca.gcms_product_identifier,
            ca.funding_source,
            ca.settlement_report_amount,
            ca.settlement_report_currency_code,
            cast(strftime(t.date_and_time_local_transaction,'%Y-%m-%d') as DATE) as txn_date,
            try_cast(t.currency_code_transaction as BIGINT) as currency_code_transaction_num
        FROM txn t
        JOIN calc ca
          ON t.file_id=ca.file_id
         AND t.ref_id=ca.ref_id
         AND t.file_idn=ca.file_idn
    ),

    work2 AS (
        SELECT
            w.*,
            cur.currency_alphabetic_code as trx_ccy_alpha
        FROM work w
        LEFT JOIN cur
          ON try_cast(cur.currency_numeric_code as BIGINT)
             = w.currency_code_transaction_num
    ),

    ex2 AS (
        SELECT
            cast(rate_date as DATE) as rate_date_d,
            upper(brand) as brand_u,
            try_cast(currency_from_code as BIGINT) as currency_from_code_num,
            upper(trim(currency_to)) as currency_to_u,
            try_cast(exchange_value as DOUBLE) as exchange_value_num
        FROM ex
    ),

    ex_pivot AS (
        SELECT
            rate_date_d,
            currency_from_code_num,
            {pivot_sql}
        FROM ex2
        GROUP BY 1,2
    )

    SELECT
        w2.file_id,
        w2.ref_id,
        w2.file_idn,

        coalesce(trim(substr(cast(w2.pan as varchar),1,8)),'BLANK') as issuer_bin_8,
        coalesce(trim(substr(cast(w2.acquirer_reference_data as varchar),2,6)),'BLANK') as acquirer_bin,

        coalesce(trim(cast(w2.jurisdiction as varchar)),'BLANK') as jurisdiction,
        coalesce(trim(cast(w2.ird as varchar)),'BLANK') as ird,

        coalesce(trim(substr(cast(w2.processing_code as varchar),1,2)),'BLANK') as processing_code,

        coalesce(cast(w2.amount_transaction as varchar),'BLANK') as amount_transaction,
        coalesce(cast(w2.settlement_report_amount as varchar),'BLANK') as settlement_report_amount,
        coalesce(cast(w2.settlement_report_currency_code as varchar),'BLANK') as settlement_report_currency_code,

        coalesce(cast(w2.trx_ccy_alpha as varchar),'BLANK') as amount_transaction_currency,

        coalesce(trim(cast(w2.card_acceptor_business_code as varchar)),'BLANK') as card_acceptor_business_code,

        coalesce(trim(cast(w2.gcms_product_identifier as varchar)),'BLANK') as gcms_product_identifier,
        coalesce(trim(cast(w2.funding_source as varchar)),'BLANK') as funding_source,
        coalesce(cast(w2.mastercard_assigned_id as varchar),'BLANK') as mastercard_assigned_id,

        w2.txn_date,
        w2.currency_code_transaction_num as currency_code_transaction

        {dyn_cols_sql}

    FROM work2 w2
    LEFT JOIN ex_pivot ex_p
      ON ex_p.rate_date_d = w2.txn_date
     AND ex_p.currency_from_code_num = w2.currency_code_transaction_num
    """

    con = duckdb.connect()
    try:
        con.register("cur", df_curr)
        con.register("ex", df_ex)

        df_eval = con.execute(sql, [p_txn, p_cal]).df()

        log.logger.debug(
            f"[calculate_pre_eval] rows={len(df_eval)} targets={len(target_ccys)}"
        )

        return df_eval

    finally:
        con.close()


# Columnas donde soportamos rangos numéricos (ej. "20000000-29999999")
# NOTA: se evalúan en SQL usando BETWEEN cuando el token se interpreta como rango.
RANGE_COLS = {"issuer_bin_8", "acquirer_bin", "card_acceptor_business_code"}


def _blanklike(v: object) -> bool:
    """Devuelve True si el valor representa "vacío" (incluye None, NaN, 'null', etc.)."""
    s = str(v).strip()
    return s == "" or s.lower() in {"none", "nan", "null"}


def _split_csv_nospace(s: str) -> list[str]:
    """
    Split CSV removiendo espacios:
    "A, B, C" -> ["A","B","C"]
    """
    return [x for x in str(s).replace(" ", "").split(",") if x != ""]


def _is_range_token(tok: str) -> bool:
    """
    Retorna True si el token luce como un rango numérico "lo-hi".
    Se usa para RANGE_COLS (evalúa con BETWEEN).
    """
    tok = str(tok).strip()
    if "-" not in tok:
        return False
    a, b = tok.split("-", 1)
    return a.strip().isdigit() and b.strip().isdigit()


def _parse_list_and_ranges(expr: str):
    """
    Convierte una expresión tipo:
      "123,124,200-210"
    en:
      vals = ["123","124"]
      rngs = [(200,210)]
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
    Parser legacy para reglas de amount_transaction.

    Formatos soportados:
      1) Comparadores: ">=10,<=20"  (múltiples condiciones separadas por coma)
      2) Between:      "between10and20" (sin espacios)
      3) Else:         igualdad exacta (texto)

    Retorna lista de predicados (para combinarlos con bool_and en SQL):
      ("between", lo, hi)
      ("cmp", op, val)
      ("eq", text)
    """
    e = str(expr).strip()
    if _blanklike(e):
        return []

    e0 = e.replace(" ", "")
    el = e0.lower()

    # Caso between10and20
    if "between" in el:
        tmp = el.replace("between", "")
        if "and" in tmp:
            a, b = tmp.split("and", 1)
            try:
                return [("between", float(a), float(b))]
            except Exception:
                return []

    # Caso comparadores >=, <=, <, >, =
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

    # Caso igualdad (texto tal cual)
    return [("eq", e0.strip())]



def _prefilter_rules_for_work(rules_raw: pd.DataFrame, work: pd.DataFrame) -> pd.DataFrame:
    w = work.copy()
    w["txn_date_d"] = pd.to_datetime(w["txn_date"], errors="coerce").dt.date
    min_dt = w["txn_date_d"].min()
    max_dt = w["txn_date_d"].max()

    jur_set = set(w["jurisdiction"].astype(str).str.strip().str.upper().unique())
    ird_set = set(w["ird"].astype(str).str.strip().str.upper().unique())

    r = rules_raw.copy()
    r["region_u"] = r["region_country_code"].astype(str).str.strip().str.upper()
    r["ird_u"] = r["ird"].astype(str).str.strip().str.upper()
    r["valid_from_d"] = pd.to_datetime(r["valid_from"], errors="coerce").dt.date
    r["valid_until_d"] = pd.to_datetime(r["valid_until"], errors="coerce").dt.date

    r = r[
        r["region_u"].isin(jur_set)
        & r["ird_u"].isin(ird_set)
        & (r["valid_from_d"] <= max_dt)
        & (r["valid_until_d"].isna() | (r["valid_until_d"] >= min_dt))
    ].copy()

    return r.drop(columns=["region_u","ird_u","valid_from_d","valid_until_d"], errors="ignore")



def _merge_extras(out_min: pd.DataFrame, work: pd.DataFrame, extras: list[str] | None) -> pd.DataFrame:
    default_extras = [
        "jurisdiction","processing_code","card_acceptor_business_code",
        "amount_transaction","amount_transaction_currency",
        "settlement_report_amount","settlement_report_currency_code",
        "txn_date",
        "issuer_bin_8","acquirer_bin","gcms_product_identifier","funding_source","mastercard_assigned_id"
    ]
    extras = default_extras if extras is None else extras
    extras_present = [c for c in extras if c in work.columns]

    out = out_min.merge(work[["id"] + extras_present], on="id", how="left") if extras_present else out_min.copy()
    return out.drop(columns=["id"], errors="ignore")

def assign_rules(
    *,
    df_eval: pd.DataFrame,
    db,
    extras: list[str] | None = None,
    partition: Optional[bool] = None,
) -> pd.DataFrame:
    """
    LONG FORMAT RULE ENGINE:
    - Prefiltro de reglas por universo de work
    - Long format para condiciones (evita CASE dinámico)
    - Partición adaptativa por (jurisdiction, ird) para no reventar RAM
    """
    if df_eval is None or df_eval.empty:
        return pd.DataFrame()

    work = df_eval.copy()
    work["id"] = range(1, len(work) + 1)

    required = {"file_id","ref_id","file_idn","jurisdiction","ird","txn_date"}
    missing = [c for c in required if c not in work.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en df_eval: {missing}")

    # Reglas
    rules_raw = pd.DataFrame(db.read_sql("SELECT * FROM mc_rules"))
    if rules_raw.empty:
        out = work[["id","file_id","ref_id","file_idn"]].copy()
        out["rule"] = 0
        out["region_country_code"] = None
        out["intelica_id"] = None
        out["ird"] = work["ird"].astype(str)
        out["rate_currency"]=None; out["rate_variable"]=None; out["rate_fixed"]=None
        out["rate_min"]=None; out["rate_cap"]=None; out["valid_from"]=pd.NaT; out["valid_until"]=pd.NaT
        return _merge_extras(out, work, extras)

    # Prefiltro (HUGE win)
    rules_raw = _prefilter_rules_for_work(rules_raw, work)
    if rules_raw.empty:
        out = work[["id","file_id","ref_id","file_idn"]].copy()
        out["rule"] = 0
        out["region_country_code"] = None
        out["intelica_id"] = None
        out["ird"] = work["ird"].astype(str)
        out["rate_currency"]=None; out["rate_variable"]=None; out["rate_fixed"]=None
        out["rate_min"]=None; out["rate_cap"]=None; out["valid_from"]=pd.NaT; out["valid_until"]=pd.NaT
        return _merge_extras(out, work, extras)

    # Orden legacy + key
    rules = rules_raw.copy()
    rules["_intelica_num"] = pd.to_numeric(rules.get("intelica_id"), errors="coerce")
    rules = rules.sort_values(["region_country_code","_intelica_num"], na_position="last").reset_index(drop=True)
    rules["key"] = range(1, len(rules)+1)

    # Cond cols usadas (las 5 que viste)
    cond_cols = [
        "processing_code",
        "card_acceptor_business_code",
        "gcms_product_identifier",
        "funding_source",
        "mastercard_assigned_id",
    ]
    cond_cols = [c for c in cond_cols if c in work.columns]

    range_cols_present = [c for c in RANGE_COLS if c in work.columns]

    # -------- build pos/neg/amt igual que antes (reusa tu parser) ----------
    excluded = {
        "app_creation_user","app_creation_date","key",
        "amount_transaction_currency","jurisdiction","region_country_code","guide_date",
        "valid_from","valid_until","fee_category","fee_tier","intelica_id","ird",
        "rate_currency","rate_variable","rate_fixed","rate_min","rate_cap",
        "masterpass_incentive_indicator","tti","additional_data",
        "_intelica_num"
    }
    rule_cols = [c for c in rules.columns if c not in excluded]

    pos_vals, neg_vals = [], []
    pos_rng,  neg_rng  = [], []
    amt_rules = []

    for _, rr in rules.iterrows():
        k = int(rr["key"])

        # amount_transaction 
        amt_expr = rr.get("amount_transaction", None)
        if amt_expr is not None and not _blanklike(amt_expr):
            ccy = str(rr.get("amount_transaction_currency", "")).strip().lower()
            for p in _parse_amount_legacy(str(amt_expr)):
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
                for lo, hi in rngs:
                    rec3 = (k, col, f"{lo}-{hi}".upper())
                    (neg_vals if is_not else pos_vals).append(rec3)

    df_pos_vals = pd.DataFrame(pos_vals, columns=["key","col","val"]) if pos_vals else pd.DataFrame(columns=["key","col","val"])
    df_neg_vals = pd.DataFrame(neg_vals, columns=["key","col","val"]) if neg_vals else pd.DataFrame(columns=["key","col","val"])
    df_pos_rng  = pd.DataFrame(pos_rng,  columns=["key","col","lo","hi"]) if pos_rng else pd.DataFrame(columns=["key","col","lo","hi"])
    df_neg_rng  = pd.DataFrame(neg_rng,  columns=["key","col","lo","hi"]) if neg_rng else pd.DataFrame(columns=["key","col","lo","hi"])
    df_amt      = pd.DataFrame(amt_rules, columns=["key","ccy","kind","op","lo","hi","eq_str"]) if amt_rules else pd.DataFrame(columns=["key","ccy","kind","op","lo","hi","eq_str"])

    # Decide partición (adaptativo)
    if partition is None:
        # heurística simple
        partition = len(work) > 50_000

    def run_block(wblk: pd.DataFrame) -> pd.DataFrame:
        # amount sql 
        amt_sql = ""
        if not df_amt.empty:
            ccys = sorted({str(x).strip().lower() for x in df_amt["ccy"].tolist() if str(x).strip()})
            case_lines = []
            for ccy in ccys:
                colname = f"amount_transaction_{ccy}"
                if colname in wblk.columns:
                    case_lines.append(f"WHEN a.ccy='{ccy}' THEN try_cast(w2.\"{colname}\" as double)")
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

        # LONG FORMAT: work_long y work_long_num se construyen dentro del SQL (solo 5 cols)
        long_union = "\nUNION ALL\n".join([
            f"SELECT id, '{c}' as col, upper(trim(cast(\"{c}\" as varchar))) as val_u FROM work2"
            for c in cond_cols
        ]) if cond_cols else "SELECT id, 'x' as col, NULL as val_u FROM work2 WHERE 1=0"

        long_union_num = "\nUNION ALL\n".join([
            f"SELECT id, '{c}' as col, try_cast(\"{c}\" as BIGINT) as val_num FROM work2"
            for c in range_cols_present
        ]) if range_cols_present else "SELECT id, 'x' as col, NULL as val_num FROM work2 WHERE 1=0"

        sql = f"""
        WITH
        work2 AS (
          SELECT
            w.*,
            try_cast(w.txn_date as DATE) AS txn_date_d,
            upper(trim(cast(w.jurisdiction as varchar))) AS jurisdiction_u,
            upper(trim(cast(w.ird as varchar))) AS ird_u
          FROM work w
        ),

        base AS (
          SELECT
            w2.id, w2.file_id, w2.ref_id, w2.file_idn, w2.txn_date_d,
            r.key,
            upper(trim(cast(r.region_country_code as varchar))) AS region_country_code_u,
            upper(trim(cast(r.ird as varchar))) AS ird_u,
            try_cast(r.intelica_id as BIGINT) AS intelica_id_num,
            r.rate_currency,
            try_cast(r.rate_variable as DOUBLE) AS rate_variable,
            try_cast(r.rate_fixed as DOUBLE)    AS rate_fixed,
            try_cast(r.rate_min as DOUBLE)      AS rate_min,
            try_cast(r.rate_cap as DOUBLE)      AS rate_cap,
            cast(r.valid_from as DATE) AS valid_from,
            cast(r.valid_until as DATE) AS valid_until
          FROM work2 w2
          JOIN rules r
            ON w2.jurisdiction_u = upper(trim(cast(r.region_country_code as varchar)))
           AND w2.ird_u          = upper(trim(cast(r.ird as varchar)))
           AND cast(r.valid_from as DATE) <= w2.txn_date_d
           AND (r.valid_until IS NULL OR cast(r.valid_until as DATE) >= w2.txn_date_d)
        ),

        work_long AS (
          {long_union}
        ),
        work_long_num AS (
          {long_union_num}
        ),

        req_pos AS (SELECT DISTINCT key, col FROM pos_vals),
        req_rng AS (SELECT DISTINCT key, col FROM pos_rng),

        -- POS: cols satisfechas (id,key,col)
        sat_pos AS (
          SELECT DISTINCT b.id, b.key, pv.col
          FROM base b
          JOIN work_long wl ON wl.id=b.id
          JOIN pos_vals pv
            ON pv.key=b.key AND pv.col=wl.col AND pv.val=wl.val_u
        ),
        pos_ok AS (
          SELECT
            b.id, b.key,
            -- cuántas cols requiere esa key
            (SELECT count(*) FROM req_pos rp WHERE rp.key=b.key) as need_pos,
            -- cuántas cols se cumplieron
            (SELECT count(*) FROM (SELECT DISTINCT col FROM sat_pos sp WHERE sp.id=b.id AND sp.key=b.key) t) as got_pos
          FROM base b
          GROUP BY 1,2
        ),

        -- NEG: si existe match -> falla
        neg_bad AS (
          SELECT DISTINCT b.id, b.key
          FROM base b
          JOIN work_long wl ON wl.id=b.id
          JOIN neg_vals nv
            ON nv.key=b.key AND nv.col=wl.col AND nv.val=wl.val_u
        ),

        -- RANGOS POS
        sat_rng AS (
          SELECT DISTINCT b.id, b.key, pr.col
          FROM base b
          JOIN work_long_num wn ON wn.id=b.id
          JOIN pos_rng pr
            ON pr.key=b.key AND pr.col=wn.col AND wn.val_num BETWEEN pr.lo AND pr.hi
        ),
        rng_ok AS (
          SELECT
            b.id, b.key,
            (SELECT count(*) FROM req_rng rr WHERE rr.key=b.key) as need_rng,
            (SELECT count(*) FROM (SELECT DISTINCT col FROM sat_rng sr WHERE sr.id=b.id AND sr.key=b.key) t) as got_rng
          FROM base b
          GROUP BY 1,2
        ),

        -- RANGOS NEG
        rng_bad AS (
          SELECT DISTINCT b.id, b.key
          FROM base b
          JOIN work_long_num wn ON wn.id=b.id
          JOIN neg_rng nr
            ON nr.key=b.key AND nr.col=wn.col AND wn.val_num BETWEEN nr.lo AND nr.hi
        ),

        filtered AS (
          SELECT b.*
          FROM base b
          LEFT JOIN pos_ok p  ON p.id=b.id AND p.key=b.key
          LEFT JOIN rng_ok r  ON r.id=b.id AND r.key=b.key
          LEFT JOIN neg_bad n ON n.id=b.id AND n.key=b.key
          LEFT JOIN rng_bad rb ON rb.id=b.id AND rb.key=b.key
          JOIN work2 w2 ON w2.id=b.id
          WHERE 1=1
            AND (coalesce(p.need_pos,0)=0 OR p.got_pos = p.need_pos)
            AND (coalesce(r.need_rng,0)=0 OR r.got_rng = r.need_rng)
            AND n.id IS NULL
            AND rb.id IS NULL
            {amt_sql}
        ),

        best AS (
          SELECT *
          FROM (
            SELECT
              f.*,
              row_number() over(partition by f.id order by f.key) as rn
            FROM filtered f
          ) x
          WHERE rn=1
        )

        SELECT
          w2.id,
          w2.file_id, w2.ref_id, w2.file_idn,
          coalesce(b.key,0) as rule,
          b.region_country_code_u as region_country_code,
          cast(b.intelica_id_num as varchar) as intelica_id,
          coalesce(b.ird_u, upper(trim(cast(w2.ird as varchar)))) as ird,
          b.rate_currency, b.rate_variable, b.rate_fixed, b.rate_min, b.rate_cap,
          b.valid_from, b.valid_until
        FROM work2 w2
        LEFT JOIN best b
          ON b.id=w2.id
        """

        con = duckdb.connect()
        try:
            con.register("work", wblk)
            con.register("rules", rules)
            con.register("pos_vals", df_pos_vals)
            con.register("neg_vals", df_neg_vals)
            con.register("pos_rng", df_pos_rng)
            con.register("neg_rng", df_neg_rng)
            con.register("amt", df_amt)
            out_min = con.execute(sql).df()
        finally:
            con.close()

        return _merge_extras(out_min, wblk, extras)

    if not partition:
        return run_block(work)

    outs = []
    for (jur, ird), g in work.groupby(["jurisdiction","ird"], sort=False):
        outs.append(run_block(g))
    return pd.concat(outs, ignore_index=True)


def calculate_mastercard_fee(
    *,
    df_assign: pd.DataFrame,
    db: Database,
    brand_fx_eval: str = "MASTERCARD",
) -> pd.DataFrame:
    """
    CÁLCULO DEL FEE (APLICA RATE_VARIABLE / RATE_FIXED + MIN/CAP + FX)

    Devuelve:
      - calculated_fee: fee en moneda de la regla (rate_currency)
      - calculated_fee_settlement: fee convertido a moneda de liquidación (settlement_report_currency_code)

    FIX:
    - Evita upper(INTEGER) normalizando rate_currency, amount_transaction_currency y settlement_report_currency_code a VARCHAR
    - Usa *_u en TODO el SQL (CASEs y JOINs)
    """

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

            -- ===== numéricos =====
            try_cast(a.amount_transaction AS DOUBLE) AS amount_transaction_num,
            try_cast(a.rate_variable AS DOUBLE)      AS rate_variable_num,
            try_cast(a.rate_fixed AS DOUBLE)         AS rate_fixed_num,
            try_cast(a.rate_min AS DOUBLE)           AS rate_min_num,
            try_cast(a.rate_cap AS DOUBLE)           AS rate_cap_num,
            try_cast(a.txn_date AS DATE)             AS txn_date_d,

            -- ===== normalizados a texto (para upper/joins sin reventar) =====
            nullif(upper(trim(cast(a.rate_currency as varchar))), '')                 AS rate_currency_u,
            nullif(upper(trim(cast(a.amount_transaction_currency as varchar))), '')   AS trx_currency_u,
            nullif(upper(trim(cast(a.settlement_report_currency_code as varchar))), '') AS settlement_currency_u
          FROM a
        ),
        ex2 AS (
          SELECT
            try_cast(ex.rate_date AS DATE) AS rate_date_d,
            upper(ex.brand) AS brand_u,
            nullif(upper(trim(cast(ex.currency_from as varchar))), '') AS currency_from_u,
            nullif(upper(trim(cast(ex.currency_to   as varchar))), '') AS currency_to_u,
            try_cast(ex.exchange_value AS DOUBLE) AS exchange_value_num
          FROM ex
        )
        SELECT
            a2.*,

            -- =========================
            -- FX txn -> rule
            -- =========================
            CASE
                WHEN a2.rate_currency_u IS NULL THEN 1.0
                WHEN a2.rate_currency_u = a2.trx_currency_u THEN 1.0
                ELSE ex_rule.exchange_value_num
            END AS fx_multiplier,

            -- =========================
            -- Amount convertido a moneda de la regla
            -- =========================
            a2.amount_transaction_num *
            CASE
                WHEN a2.rate_currency_u IS NULL THEN 1.0
                WHEN a2.rate_currency_u = a2.trx_currency_u THEN 1.0
                ELSE ex_rule.exchange_value_num
            END AS amount_converted,

            -- =========================
            -- Fee preliminar (en moneda de la regla)
            -- =========================
            (
                coalesce(a2.rate_variable_num,0.0) *
                (
                    a2.amount_transaction_num *
                    CASE
                        WHEN a2.rate_currency_u IS NULL THEN 1.0
                        WHEN a2.rate_currency_u = a2.trx_currency_u THEN 1.0
                        ELSE ex_rule.exchange_value_num
                    END
                )
            )
            + coalesce(a2.rate_fixed_num,0.0) AS fee_preliminary,

            -- =========================
            -- Fee final (min/cap) en moneda de la regla
            -- =========================
            CASE
                WHEN a2.rate_variable IS NULL
                    THEN coalesce(a2.rate_fixed_num,0.0)

                WHEN a2.rate_variable_num IS NULL THEN NULL
                WHEN a2.amount_transaction_num IS NULL THEN NULL

                -- si se necesitaba FX (txn->rule) y no existe, devuelve NULL
                WHEN a2.rate_currency_u IS NOT NULL
                 AND a2.rate_currency_u <> a2.trx_currency_u
                 AND ex_rule.exchange_value_num IS NULL
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
                                        WHEN a2.rate_currency_u IS NULL THEN 1.0
                                        WHEN a2.rate_currency_u = a2.trx_currency_u THEN 1.0
                                        ELSE ex_rule.exchange_value_num
                                    END
                                )
                            )
                            + coalesce(a2.rate_fixed_num,0.0)
                        )
                    )
            END AS calculated_fee,

            -- =========================
            -- FX rule -> settlement
            -- =========================
            CASE
                WHEN a2.settlement_currency_u IS NULL THEN 1.0
                WHEN a2.rate_currency_u IS NULL THEN 1.0
                WHEN a2.rate_currency_u = a2.settlement_currency_u THEN 1.0
                ELSE ex_settle.exchange_value_num
            END AS fx_rule_to_settlement,

            -- =========================
            -- Fee convertido a moneda de liquidación
            -- =========================
            CASE
                -- si no hay moneda settlement, dejamos el fee en moneda de regla
                WHEN a2.settlement_currency_u IS NULL THEN calculated_fee
                -- si la regla no define moneda, no convertimos
                WHEN a2.rate_currency_u IS NULL THEN calculated_fee
                -- si ya está en la misma moneda
                WHEN a2.rate_currency_u = a2.settlement_currency_u THEN calculated_fee
                -- si se necesita FX y no existe
                WHEN ex_settle.exchange_value_num IS NULL THEN NULL
                ELSE calculated_fee * ex_settle.exchange_value_num
            END AS calculated_fee_settlement

        FROM a2

        -- FX txn -> rule
        LEFT JOIN ex2 ex_rule
          ON ex_rule.rate_date_d = a2.txn_date_d
         AND ex_rule.currency_from_u = a2.trx_currency_u
         AND ex_rule.currency_to_u   = a2.rate_currency_u
         AND ex_rule.brand_u = upper('{brand_fx_eval}')

        -- FX rule -> settlement
        LEFT JOIN ex2 ex_settle
          ON ex_settle.rate_date_d = a2.txn_date_d
         AND ex_settle.currency_from_u = a2.rate_currency_u
         AND ex_settle.currency_to_u   = a2.settlement_currency_u
         AND ex_settle.brand_u = upper('{brand_fx_eval}')
        """

        out_dir = Path.cwd() / "debug_rule_engine"   # se crea en tu carpeta actual (normalmente el repo) / "debug_rule_engine"
        out_dir.mkdir(exist_ok=True)
      
        df_base_fin = con.execute(sql).df()
        df_base_fin.to_csv(out_dir / "df_base_fin.csv", index=False)

        return con.execute(sql).df()

    finally:
        con.close()
