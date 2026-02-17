import pandas as pd
import duckdb
from pathlib import Path
from interchange.persistence.database import Database
from interchange.logs.logger import Logger
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
) -> pd.DataFrame:
    """
    PRE-EVALUACIÓN (ARMADO DEL DATASET PARA ASIGNAR REGLAS)

    Qué hace:
    - Lee 2 parquets: transaccional (txn) y calculado (calc)
    - Une ambos por (file_id, ref_id)
    - Mapea moneda numérica (DE49) -> código alfabético usando la tabla currency
    - Obtiene desde mc_rules las monedas objetivo que se usan en reglas de monto
    - Construye columnas dinámicas amount_transaction_{CCY} para cada moneda objetivo,
      usando exchange_rate por fecha + moneda origen (numérica) + moneda destino (alpha)

    Output:
    - DataFrame df_eval listo para alimentar assign_rules().
    """

    # Normaliza rutas a string absoluto (DuckDB read_parquet() acepta paths)
    p_txn = str(Path(parquet_txn_path).resolve())
    p_cal = str(Path(parquet_calc_path).resolve())

    # 1) Monedas objetivo desde reglas
    #    Si en mc_rules hay reglas de amount_transaction para varias monedas (USD, EUR, etc.),
    #    aquí las extraemos para generar amount_transaction_USD, amount_transaction_EUR, ...
    df_targets = db.read_sql("""
    SELECT upper(trim(amount_transaction_currency)) AS amount_transaction_currency
    FROM mc_rules
    WHERE amount_transaction IS NOT NULL
      AND trim(cast(amount_transaction as varchar)) <> ''
      AND amount_transaction_currency IS NOT NULL
      AND trim(cast(amount_transaction_currency as varchar)) <> ''
    GROUP BY 1
    """)
    target_ccys = df_targets["amount_transaction_currency"].dropna().astype(str).str.strip().tolist()

    # 2) Currency dim
    #    Mapea currency_numeric_code -> currency_alphabetic_code
    #    (ej. 840 -> USD). Se usa para la moneda base de la transacción.
    df_curr = db.read_sql("""
        SELECT currency_numeric_code, currency_alphabetic_code
        FROM currency
    """)

    # 3) Exchange rates
    #    Se consulta solo para la marca (brand_fx_eval), para luego construir los montos convertidos
    #    amount_transaction_{CCY} desde amount_transaction.
    #    NOTA: aquí usas currency_from_code (numérico) + currency_to (alpha)
    df_ex = db.read_sql("""
        SELECT
            rate_date,          -- fecha del tipo de cambio
            brand,
            currency_from_code, -- moneda origen (numérica)
            currency_to,        -- moneda destino (alfabética)
            exchange_value
        FROM exchange_rate
        WHERE upper(brand)=upper(?)
    """, params=(brand_fx_eval,))

    # Conexión DuckDB in-memory (solo para ejecutar la query sobre parquets + dataframes registrados)
    con = duckdb.connect()
    try:
        # Registra dataframes como tablas dentro de DuckDB
        con.register("cur", df_curr)
        con.register("ex", df_ex)

        # Construcción dinámica:
        # - dyn_cols: columnas SELECT calculadas amount_transaction_{CCY}
        # - dyn_joins: LEFT JOIN por cada CCY objetivo (estilo legacy)
        dyn_cols = []
        dyn_joins = []

        for ccy in target_ccys:

            ccy_u = ccy.upper().strip()
            ccy_l = ccy_u.lower()
            alias = f"ex_{ccy_l}"  # alias único por moneda para no chocar en SQL

            # Columna dinámica: amount_transaction_{ccy}
            # - Si la moneda base ya es la moneda objetivo => multiplica por 1
            # - Si no, usa exchange_value del join correspondiente
            # - Se castea a VARCHAR y se coalesce a 'BLANK' para mantener semántica legacy
            dyn_cols.append(
                f""",
                COALESCE(
                    CAST(
                        (t.amount_transaction *
                            CASE
                                WHEN upper(cur.currency_alphabetic_code) = upper('{ccy_u}') THEN 1
                                ELSE {alias}.exchange_value
                            END
                        ) AS VARCHAR
                    ),
                    'BLANK'
                ) AS amount_transaction_{ccy_l}
                """
            )

            # Join FX por:
            # - fecha (txn_date sacada del timestamp local)
            # - moneda origen numérica (DE49) vs exchange_rate.currency_from_code
            # - moneda destino alpha (exchange_rate.currency_to = ccy)
            # Esto replica el patrón legacy de "un LEFT JOIN por moneda objetivo".
            dyn_joins.append(
                f"""
                LEFT JOIN ex {alias}
                  ON CAST({alias}.rate_date AS DATE) = CAST(strftime(t.date_and_time_local_transaction, '%Y-%m-%d') AS DATE)
                 AND try_cast({alias}.currency_from_code AS BIGINT) = try_cast(t.currency_code_transaction AS BIGINT)
                 AND upper({alias}.currency_to) = upper('{ccy_u}')
                """
            )

        # Une lo construido (si no hay target_ccys, ambos quedan vacíos y no se inyecta nada)
        dyn_cols_sql = "\n".join(dyn_cols)
        dyn_joins_sql = "\n".join(dyn_joins)

        # Query principal:
        # - txn: lee parquet transaccional y selecciona campos necesarios para reglas
        # - calc: lee parquet calculado (jurisdiction, producto, funding, settlement fields)
        # - SELECT final: estandariza/castea a texto y aplica COALESCE(...,'BLANK') como legacy
        sql = f"""
        WITH txn AS (
            SELECT
                file_id,
                ref_id,
                file_idn,
                pan_de_2 AS pan,                         -- usado para issuer_bin_8
                acquirer_reference_data_de_31 AS acquirer_reference_data,    
                -- electronic_commerce_indicator_3,       -- comentado: opcional
                processing_code_de_3 AS processing_code,
                "card_acceptor_business_code_[mcc]_de_26" AS card_acceptor_business_code,
                date_and_time_local_transaction_de_12 AS date_and_time_local_transaction,
                business_activity_4_pds_158_4 AS ird, 
                amount_transaction_de_4 as amount_transaction,
                currency_code_transaction_de_49 as currency_code_transaction,
                mastercard_assigned_id_pds_176 as mastercard_assigned_id        -- opcional para reglas
            FROM read_parquet(?)
        ),
        calc AS (
            SELECT
                file_id,
                ref_id,
                file_idn,
                jurisdiction_assigned AS jurisdiction,
                gcms_product_identifier,      -- opcional para reglas
                funding_source,               -- opcional para reglas
                settlement_report_amount,
                settlement_report_currency_code
            FROM read_parquet(?)
        )
        SELECT
            -- =========================
            -- Identificadores
            -- =========================
            t.file_id,
            t.ref_id,
            t.file_idn,
            -- =========================
            -- Campos base para reglas (estilo legacy)
            -- =========================
            COALESCE(TRIM(SUBSTR(CAST(t.pan AS VARCHAR), 1, 8)), 'BLANK') AS issuer_bin_8,
            COALESCE(TRIM(SUBSTR(CAST(t.acquirer_reference_data AS VARCHAR), 2, 6)), 'BLANK') AS acquirer_bin,
            -- COALESCE(TRIM(CAST(t.electronic_commerce_indicator_3 AS VARCHAR)), 'BLANK') AS electronic_commerce_indicator_3,

            COALESCE(TRIM(CAST(ca.jurisdiction AS VARCHAR)), 'BLANK') AS jurisdiction,
            COALESCE(TRIM(CAST(t.ird AS VARCHAR)), 'BLANK') AS ird,

            -- processing_code: se queda con los 2 primeros dígitos
            COALESCE(TRIM(SUBSTR(CAST(t.processing_code AS VARCHAR), 1, 2)), 'BLANK') AS processing_code,

            -- Montos/campos auxiliares (guardados como texto para compatibilidad legacy)
            COALESCE(CAST(t.amount_transaction AS VARCHAR), 'BLANK') AS amount_transaction,
            COALESCE(CAST(settlement_report_amount AS VARCHAR), 'BLANK') settlement_report_amount,
            COALESCE(CAST(settlement_report_currency_code AS VARCHAR), 'BLANK') settlement_report_currency_code,
            COALESCE(CAST(cur.currency_alphabetic_code AS VARCHAR), 'BLANK') AS amount_transaction_currency,

            COALESCE(TRIM(CAST(t.card_acceptor_business_code AS VARCHAR)), 'BLANK') AS card_acceptor_business_code,

            -- =========================
            -- Campos opcionales para reglas
            -- =========================
            COALESCE(TRIM(CAST(ca.gcms_product_identifier AS VARCHAR)), 'BLANK') AS gcms_product_identifier,
            COALESCE(TRIM(CAST(ca.funding_source AS VARCHAR)), 'BLANK') AS funding_source,
            COALESCE(CAST(t.mastercard_assigned_id AS VARCHAR), 'BLANK') AS mastercard_assigned_id,

            -- =========================
            -- Campos para vigencia / FX posterior
            -- =========================
            CAST(strftime(t.date_and_time_local_transaction, '%Y-%m-%d') AS DATE) AS txn_date,
            try_cast(t.currency_code_transaction AS BIGINT) AS currency_code_transaction

            -- =========================
            -- Columnas dinámicas amount_transaction_CCY
            -- =========================
            {dyn_cols_sql}

        FROM txn t
        INNER JOIN calc ca
          ON t.file_id = ca.file_id
         AND t.ref_id  = ca.ref_id
         AND t.file_idn = ca.file_idn

        -- Mapea moneda numérica -> alpha de la transacción
        LEFT JOIN cur
          ON try_cast(cur.currency_numeric_code AS BIGINT) = try_cast(t.currency_code_transaction AS BIGINT)

        -- Joins dinámicos a exchange_rate (uno por cada moneda objetivo)
        {dyn_joins_sql}
        """

        # Ejecuta query con parámetros: [parquet_txn, parquet_calc]
        df_eval = con.execute(sql, [p_txn, p_cal]).df()
        log.logger.debug(f"[calculate_pre_eval_full_legacy] rows={len(df_eval)} targets={len(target_ccys)}")

        out_dir = Path.cwd() / "debug_rule_engine"
        out_dir.mkdir(parents=True, exist_ok=True)
        df_eval.to_csv(out_dir / "pre_eval_df_eval_debug.csv", index=False)

        return df_eval

    finally:
        # Cierra conexión DuckDB pase lo que pase
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


def assign_rules(
    *,
    df_eval: pd.DataFrame,
    db,
    extras: list[str] | None = None,
) -> pd.DataFrame:
    """
    ASIGNACIÓN DE REGLAS (ESTILO LEGACY) + VERSIONADO POR VIGENCIA

    Idea general:
      1) DuckDB retorna SOLO lo necesario por transacción:
         id, rule(key), region_country_code, intelica_id, ird, rate_*, valid_*
      2) Luego hacemos merge con df_eval para traer columnas extra (si existen),
         sin depender de un SELECT gigante al final.

    Semántica legacy preservada:
      - first-match-wins por key (prioridad: region_country_code, luego intelica_id num)
      - soporta NOT:, listas, y rangos en RANGE_COLS (sin expandir rangos)
      - amount_transaction evalúa amount_transaction_{ccy} si existe en df_eval
      - congela la versión vigente por txn_date (valid_from/valid_until)

    Requiere df_eval con:
      file_id, ref_id, jurisdiction, ird, txn_date
    """

    # Si no hay data, devuelve el esquema mínimo vacío
    if df_eval is None or df_eval.empty:
        return pd.DataFrame(columns=[
            "file_id", "ref_id", "file_idn", "rule", "region_country_code", "intelica_id", "ird",
            "rate_currency", "rate_variable", "rate_fixed", "rate_min", "rate_cap",
            "valid_from", "valid_until",
        ])

    # Work = df_eval + id artificial para usar como PK interna en joins/partition
    work = df_eval.copy()
    
   
    # filtrar solo el ref_id que quieres analizar
    # work["_ref_norm"] = work["ref_id"].astype(str).str.replace(",", "").str.strip()
    # work = work[work["_ref_norm"] == "308245"].copy()

    work["id"] = range(1, len(work) + 1)

    # Validación de columnas mínimas necesarias
    required = {"file_id", "ref_id", "file_idn","jurisdiction", "ird", "txn_date"}
    missing_req = [c for c in required if c not in work.columns]
    if missing_req:
        raise ValueError(f"df_eval debe incluir {sorted(required)}. Faltan: {missing_req}")

    # Carga reglas desde DB
    rules_raw = pd.DataFrame(db.read_sql("SELECT * FROM mc_rules"))
    if rules_raw.empty:
        # Si no hay reglas, devolvemos rule=0 y rates null
        out = work[["id", "file_id", "ref_id"]].copy()
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
        return _merge_extras(out, work, extras)
    
    # pd.set_option("display.max_columns", None)
    # pd.set_option("display.width", None)
    # pd.set_option("display.max_colwidth", None)

    #VALIDAR REGLAS
    # print(
    #     rules_raw[
    #         (rules_raw["ird"] == "EV") &
    #         (rules_raw["region_country_code"] == '5') &
    #         (rules_raw["intelica_id"] == 'EV')
    #     ].head(10)
    # )
    
    # PRIORIDAD LEGACY:
    # - se ordena por region_country_code y por intelica_id numérico
    # - se asigna key incremental; el menor key tiene mayor prioridad
    rules = rules_raw.copy()
    rules["_intelica_num"] = pd.to_numeric(rules.get("intelica_id"), errors="coerce")
    rules = rules.sort_values(["region_country_code", "_intelica_num"], na_position="last").reset_index(drop=True)
    rules["key"] = range(1, len(rules) + 1)     

    # Excluye campos no-condición (metadatos / salidas / etc.)
    excluded = {
        "app_creation_user", "app_creation_date", "key",
        "amount_transaction_currency", "jurisdiction", "region_country_code", "guide_date",
        "valid_from", "valid_until", "fee_category", "fee_tier", "intelica_id", "ird",
        "rate_currency", "rate_variable", "rate_fixed", "rate_min", "rate_cap",
        "masterpass_incentive_indicator", "tti", "additional_data",
        "_intelica_num"
    }
    # rule_cols = columnas que representan condiciones (filtros)
  
    #['processing_code', 'amount_transaction', 'card_acceptor_business_code', 'gcms_product_identifier', 'funding_source', 'mastercard_assigned_id', 'issuer_bin_8', 'acquirer_bin']
    rule_cols = [c for c in rules.columns if c not in excluded]

    # Normalización de condiciones:
    # - pos_vals / neg_vals: filtros tipo lista (incluye NOT:)
    # - pos_rng  / neg_rng : filtros tipo rango numérico para RANGE_COLS
    # - amt_rules          : filtros del monto (amount_transaction)
    pos_vals, neg_vals = [], []
    pos_rng,  neg_rng  = [], []
    amt_rules = []

    for _, rr in rules.iterrows():
        k = int(rr["key"])

        # -------------------------
        # amount_transaction
        # -------------------------
        amt_expr = rr.get("amount_transaction", None)
        if amt_expr is not None and not _blanklike(amt_expr):
            # ccy define qué columna dinámica usar: amount_transaction_{ccy}
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

        # -------------------------
        # Resto de columnas de condición
        # -------------------------
        for col in rule_cols:
            if col == "amount_transaction":
                continue

            v = rr.get(col, None)
            if v is None or _blanklike(v):
                # condición vacía => no filtra (se ignora)
                continue

            # Remueve espacios (legacy), detecta prefijo NOT:
            s = str(v).replace(" ", "")
            is_not = s.startswith("NOT:")
            if is_not:
                s = s.replace("NOT:", "").strip()

            # Separa tokens (vals) y rangos (rngs)
            vals, rngs = _parse_list_and_ranges(s)

            # Valores (igualdad / lista)
            for val in vals:
                rec = (k, col, str(val).strip().upper())
                (neg_vals if is_not else pos_vals).append(rec)

            # Rangos: solo en columnas RANGE_COLS se evalúan como BETWEEN
            if col in RANGE_COLS:
                for lo, hi in rngs:
                    rec2 = (k, col, int(lo), int(hi))
                    (neg_rng if is_not else pos_rng).append(rec2)
            else:
                # Si llega "rango" en col no-range, se trata como literal "lo-hi"
                for lo, hi in rngs:
                    rec3 = (k, col, f"{lo}-{hi}".upper())
                    (neg_vals if is_not else pos_vals).append(rec3)

    # Construye dataframes auxiliares que DuckDB usará en la query de matching
    df_pos_vals = pd.DataFrame(pos_vals, columns=["key", "col", "val"]) if pos_vals else pd.DataFrame(columns=["key", "col", "val"])
    df_neg_vals = pd.DataFrame(neg_vals, columns=["key", "col", "val"]) if neg_vals else pd.DataFrame(columns=["key", "col", "val"])
    df_pos_rng  = pd.DataFrame(pos_rng,  columns=["key", "col", "lo", "hi"]) if pos_rng else pd.DataFrame(columns=["key", "col", "lo", "hi"])
    df_neg_rng  = pd.DataFrame(neg_rng,  columns=["key", "col", "lo", "hi"]) if neg_rng else pd.DataFrame(columns=["key", "col", "lo", "hi"])
    df_amt      = pd.DataFrame(amt_rules, columns=["key", "ccy", "kind", "op", "lo", "hi", "eq_str"]) if amt_rules else pd.DataFrame(columns=["key", "ccy", "kind", "op", "lo", "hi", "eq_str"])

    
    out_dir = Path("/home/ameza/IntelicaProyectos/standard-2.0/interchange-standar-2.0/tst")

    # df_pos_vals.to_csv(out_dir / f"df_pos_vals.csv", index=False)
    # df_neg_vals.to_csv(out_dir / f"df_neg_vals.csv", index=False)
    # df_pos_rng.to_csv(out_dir / f"df_pos_rng.csv", index=False)
    # df_neg_rng.to_csv(out_dir / f"df_neg_rng.csv", index=False)
    # df_amt.to_csv(out_dir / f"df_amt.csv", index=False)
    
    # cond_cols = columnas que efectivamente aparecen en pos/neg y existen en work
    cond_cols = sorted(set(df_pos_vals["col"].unique()).union(set(df_neg_vals["col"].unique())))
    cond_cols = [c for c in cond_cols if c in work.columns]

    # range_cols_present = RANGE_COLS que existen en work (por si alguna no viene)
    range_cols_present = [c for c in RANGE_COLS if c in work.columns]

    # -------------------------
    # amount SQL block (opcional)
    # -------------------------
    # Si hay reglas de monto, creamos una condición SQL que evalúa todas las
    # restricciones de amount_transaction de la regla usando bool_and(...)
    amt_sql = ""
    if not df_amt.empty:
        # Monedas usadas en reglas de monto
        ccys = sorted({str(x).strip().lower() for x in df_amt["ccy"].tolist() if str(x).strip()})
        case_lines = []
        for ccy in ccys:
            # Columna esperada en work: amount_transaction_{ccy}
            colname = f"amount_transaction_{ccy}"
            if colname in work.columns:
                # Selecciona el monto correcto según la moneda de la regla
                case_lines.append(f"WHEN a.ccy = '{ccy}' THEN try_cast(w2.\"{colname}\" as double)")

        if case_lines:
            amt_val_expr = "CASE " + " ".join(case_lines) + " ELSE NULL END"
            amt_sql = f"""
            AND (
              -- si la regla no tiene condiciones de monto -> pasa
              (select count(*) from amt a0 where a0.key=b.key) = 0
              OR (
                -- si tiene condiciones de monto, TODAS deben cumplir (bool_and)
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
            # Si hay reglas de monto pero no existen columnas amount_transaction_{ccy} en work,
            # se fuerza a que no filtre por monto (solo pasa si no hay condiciones por key).
            amt_sql = "AND ((select count(*) from amt a0 where a0.key=b.key) = 0)"

    # -------------------------
    # CASE dinámicos para comparar contra pos_vals/neg_vals y pos_rng/neg_rng
    # -------------------------
    # Como pos_vals/neg_vals contienen (key,col,val), necesitamos obtener el valor de la
    # columna correspondiente de work2 en forma genérica, según pv.col / nv.col.
    def _case_value(ref_col: str) -> str:
        lines = [f"WHEN {ref_col} = '{c}' THEN w2.\"{c}_u\"" for c in cond_cols]
        return "CASE " + " ".join(lines) + " ELSE NULL END"

    # Similar pero para columnas numéricas (rangos)
    def _case_range(ref_col: str) -> str:
        lines = [f"WHEN {ref_col} = '{c}' THEN w2.\"{c}_num\"" for c in range_cols_present]
        return "CASE " + " ".join(lines) + " ELSE NULL END"

    case_pos_val = _case_value("pv.col")
    case_neg_val = _case_value("nv.col")
    case_pos_rng = _case_range("pr.col")
    case_neg_rng = _case_range("nr.col")

    # Precomputos en work2:
    # - {col}_u  = valor upper(trim(cast(...))) para comparación textual
    # - {col}_num= valor BIGINT para comparación por rangos (solo RANGE_COLS)
    extra_u = "".join([f', upper(trim(cast(w."{c}" as varchar))) as "{c}_u"' for c in cond_cols])
    extra_num = "".join([f', try_cast(w."{c}" as BIGINT) as "{c}_num"' for c in range_cols_present])

    # Query principal de matching:
    # - work2: normaliza jurisdicción/ird y precalcula columnas _u/_num
    # - base: candidatos por match duro jurisdiction + ird (reduce espacio)
    # - best_rule: aplica filtros (pos/neg + rangos + amount) y elige 1 regla por id (rn=1)
    # - rver: versiones (valid_from/valid_until) + rates
    # - best_with_rates: selecciona la versión vigente (rn2=1) por txn_date
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
    w2.id,
    w2.file_id,
    w2.ref_id,
    w2.file_idn,
    w2.txn_date_d,

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

best_rule AS (
  SELECT *
  FROM (
    SELECT
      b.*,

      row_number() over (
        partition by b.id
        order by b.key
      ) as rn

    FROM base b
    JOIN work2 w2 ON w2.id = b.id

    WHERE 1=1

      -- 1) POSITIVOS
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

      -- 2) NEGATIVOS
      AND NOT EXISTS (
        SELECT 1
        FROM neg_vals nv
        WHERE nv.key = b.key
          AND {case_neg_val} = nv.val
      )

      -- 3) POSITIVOS RANGO
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

      -- 4) NEGATIVOS RANGO
      AND NOT EXISTS (
        SELECT 1
        FROM neg_rng nr
        WHERE nr.key = b.key
          AND {case_neg_rng} BETWEEN nr.lo AND nr.hi
      )

      -- 5) MONTO
      {amt_sql}

  ) x
  WHERE rn = 1
)

SELECT
  w2.id,
  w2.file_id,
  w2.ref_id,
  w2.file_idn,

  coalesce(br.key, 0) AS rule,
  br.region_country_code_u AS region_country_code,
  cast(br.intelica_id_num as varchar) AS intelica_id,
  coalesce(br.ird_u, upper(trim(cast(w2.ird as varchar)))) AS ird,

  br.rate_currency,
  br.rate_variable,
  br.rate_fixed,
  br.rate_min,
  br.rate_cap,
  br.valid_from,
  br.valid_until

FROM work2 w2
LEFT JOIN best_rule br
  ON br.id = w2.id
"""

    # Ejecuta matching en DuckDB con work/rules y tablas auxiliares
    con = duckdb.connect()
    try:
        con.register("work", work)
        con.register("rules", rules)
        con.register("pos_vals", df_pos_vals)
        con.register("neg_vals", df_neg_vals)
        con.register("pos_rng", df_pos_rng)
        con.register("neg_rng", df_neg_rng)
        con.register("amt", df_amt)

        debug_base_sql = """
WITH
work2 AS (
  SELECT
    w.*,
    upper(trim(cast(w.jurisdiction as varchar))) AS jurisdiction_u,
    upper(trim(cast(w.ird as varchar))) AS ird_u
  FROM work w
)
SELECT
  w2.id,
  w2.file_id,
  w2.ref_id,
  r.key,
  r.region_country_code,
  r.ird
FROM work2 w2
JOIN rules r
  ON w2.jurisdiction_u = upper(trim(cast(r.region_country_code as varchar)))
 AND w2.ird_u          = upper(trim(cast(r.ird as varchar)))
"""
       
        out_dir = Path.cwd() / "debug_rule_engine"   # se crea en tu carpeta actual (normalmente el repo) / "debug_rule_engine"
        out_dir.mkdir(exist_ok=True)
        with open(out_dir / "full_query_debug.sql", "w", encoding="utf-8") as f:
            f.write(sql)
        
        out_min = con.execute(sql).df() # esto se mantiene
        
        df_base_debug = con.execute(debug_base_sql).df()
        df_base_debug.to_csv(out_dir / "debug_base_candidates.csv", index=False)
        
        out_min.to_csv(out_dir / "out_min_debug.csv", index=False)
    finally:
        con.close()

    # Merge de columnas extra desde work al resultado mínimo
    return _merge_extras(out_min, work, extras)


def _merge_extras(out_min: pd.DataFrame, work: pd.DataFrame, extras: list[str] | None) -> pd.DataFrame:
    """
    Merge final para traer columnas adicionales (extras) desde work.

    Motivación:
    - El SQL de assign_rules devuelve solo el mínimo (id + regla + rates).
    - Para no inflar el SQL con columnas "de salida", aquí se hace merge por id.
    """

    # Extras por defecto que normalmente se quiere retornar junto con la regla
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

    # Mantiene solo columnas extras que realmente existen en work
    extras_present = [c for c in extras if c in work.columns]

    if extras_present:
        out = out_min.merge(
            work[["id"] + extras_present],
            on="id",
            how="left",
        )
    else:
        out = out_min.copy()

    # Limpieza: id es solo PK interna
    if "id" in out.columns:
        out = out.drop(columns=["id"])

    return out

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
