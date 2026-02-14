import pandas as pd
import duckdb
from pathlib import Path
from interchange.persistence.database import Database
from interchange.logs.logger import Logger

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
                        (ca.amount_transaction *
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
                 AND try_cast({alias}.currency_from_code AS BIGINT) = try_cast(ca.currency_code_transaction AS BIGINT)
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
                card_acceptor_business_code_[mcc]_de_26 AS card_acceptor_business_code,
                date_and_time_local_transaction_de_12 AS date_and_time_local_transaction,
                business_activity_4_pds_158_4 AS ird
            FROM read_parquet(?)
        ),
        calc AS (
            SELECT
                file_id,
                ref_id,
                amount_transaction,
                currency_code_transaction,
                jurisdiction_country AS jurisdiction,
                gcms_product_identifier,      -- si lo usas en reglas
                funding_source            -- si lo usas en reglas
                --mastercard_assigned_id        -- si lo usas en reglas
            FROM read_parquet(?)
        )
        SELECT
            -- ids
            t.file_id,
            t.ref_id,

            -- campos para reglas (legacy)
            COALESCE(TRIM(SUBSTR(CAST(t.pan AS VARCHAR), 1, 8)), 'BLANK') AS issuer_bin_8,
            COALESCE(TRIM(SUBSTR(CAST(t.acquirer_reference_data AS VARCHAR), 2, 6)), 'BLANK') AS acquirer_bin,
            COALESCE(TRIM(CAST(t.electronic_commerce_indicator_3 AS VARCHAR)), 'BLANK') AS electronic_commerce_indicator_3,

            COALESCE(TRIM(CAST(ca.jurisdiction AS VARCHAR)), 'BLANK') AS jurisdiction,
            COALESCE(TRIM(CAST(t.ird AS VARCHAR)), 'BLANK') AS ird,

            COALESCE(TRIM(SUBSTR(CAST(t.processing_code AS VARCHAR), 1, 2)), 'BLANK') AS processing_code,
            COALESCE(CAST(ca.amount_transaction AS VARCHAR), 'BLANK') AS amount_transaction,

            COALESCE(CAST(cur.currency_alphabetic_code AS VARCHAR), 'BLANK') AS amount_transaction_currency,

            COALESCE(TRIM(CAST(t.card_acceptor_business_code AS VARCHAR)), 'BLANK') AS card_acceptor_business_code,

            -- opcionales para reglas
            COALESCE(TRIM(CAST(ca.gcms_product_identifier AS VARCHAR)), 'BLANK') AS gcms_product_identifier,
            COALESCE(TRIM(CAST(ca.funding_source AS VARCHAR)), 'BLANK') AS funding_source,
            COALESCE(CAST(ca.mastercard_assigned_id AS VARCHAR), 'BLANK') AS mastercard_assigned_id,

            -- para vigencia / FX posterior
            CAST(strftime(t.date_and_time_local_transaction, '%Y-%m-%d') AS DATE) AS txn_date,
            try_cast(ca.currency_code_transaction AS BIGINT) AS currency_code_transaction

            {dyn_cols_sql}

        FROM txn t
        INNER JOIN calc ca
          ON t.file_id = ca.file_id
         AND t.ref_id  = ca.ref_id

        LEFT JOIN cur
          ON try_cast(cur.currency_numeric_code AS BIGINT) = try_cast(ca.currency_code_transaction AS BIGINT)

        {dyn_joins_sql}
        """

        df_eval = con.execute(sql, [p_txn, p_cal]).df()
        log.logger.debug(f"[calculate_pre_eval_full_legacy] rows={len(df_eval)} targets={len(target_ccys)}")
        return df_eval

    finally:
        con.close()


# ============================================================
# PASO 2 (LEGACY): asignación de regla (SIN CAMBIOS)
# ============================================================
def assign_rules_legacy(
    *,
    df_eval: pd.DataFrame,
    # aquí llamas tu función tal cual existe hoy
    rule_assigner_fn,
    table_schema_source: str = "temporal",
    table_name_source: str = "mastercard_interchange_eval",
    string_date: str = "",
) -> pd.DataFrame:
    """
    Wrapper: usa tu mastercard_interchange_rule_assign TAL CUAL.
    Como tu función actual trabaja leyendo/escribiendo tablas, tú aquí adaptas.
    Si ya la migraste a DF->DF, más fácil todavía.
    """
    # Si tu assigner es DF->DF, haz:
    # return rule_assigner_fn(df_eval, string_date=string_date)

    # Si tu assigner está acoplado a tabla, aquí tú decides:
    # - escribir df_eval a temporal
    # - ejecutar assigner
    # - leer tabla assign
    raise NotImplementedError("Conecta aquí tu mastercard_interchange_rule_assign() sin cambiar su lógica.")


# ============================================================
# PASO 3 : cálculo fee usando assign + rules + fx
# ============================================================
def calculate_interchange_fee_duckdb_legacy(
    *,
    df_txn: pd.DataFrame,        # o puedes volver a leer parquet si prefieres
    df_assign: pd.DataFrame,     # salida del assigner legacy
    db: Database,
    brand_fx_fee: str = "MASTERCARD",
    string_date: str | None = None,  # fecha proceso para vigencia (si aplica)
) -> pd.DataFrame:
    """
    Replica el SQL final del legacy:
    - txn + assign (intelica_id, region_country_code, ird)
    - join a mc_rules por intelica_id + region_country_code + ird + vigencia
    - join a exchange_rate por fecha + currency_from + currency_to=rate_currency + brand=Mastercard
    - calcula fee con variable/fijo/min/cap
    """

    if df_txn.empty or df_assign.empty:
        return pd.DataFrame()

    # rules + fx + currency
    df_rules = db.read_sql("""
        SELECT
            intelica_id,
            region_country_code,
            ird,
            rate_currency,
            rate_variable,
            rate_fixed,
            rate_min,
            rate_cap,
            valid_from,
            valid_until
        FROM mc_rules
    """)

    df_ex = db.read_sql("""
        SELECT
            rate_date,
            brand,
            currency_from_code,
            currency_to,
            exchange_value
        FROM exchange_rate
        WHERE upper(brand)=upper(?)
    """, params=(brand_fx_fee,))

    df_curr = db.read_sql("""
        SELECT currency_numeric_code, currency_alphabetic_code
        FROM currency
    """)

    con = duckdb.connect()
    try:
        con.register("t", df_txn)
        con.register("a", df_assign)
        con.register("r", df_rules)
        con.register("ex", df_ex)
        con.register("cur", df_curr)

        # Si no pasas string_date, usamos txn_date
        # Si pasas string_date (yyyymmdd), filtra vigencia con esa fecha como en legacy
        where_valid = """
          CAST(t.txn_date AS DATE) BETWEEN CAST(r.valid_from AS DATE)
          AND COALESCE(CAST(r.valid_until AS DATE), CAST(t.txn_date AS DATE))
        """
        if string_date:
            where_valid = f"""
              CAST(strptime('{string_date}', '%Y%m%d') AS DATE) BETWEEN CAST(r.valid_from AS DATE)
              AND COALESCE(CAST(r.valid_until AS DATE), current_date)
            """

        sql = f"""
        WITH t2 AS (
          SELECT
            t.*,
            cur.currency_alphabetic_code AS txn_currency_alpha
          FROM t
          LEFT JOIN cur
            ON try_cast(cur.currency_numeric_code AS BIGINT) = try_cast(t.currency_code_transaction AS BIGINT)
        )
        SELECT DISTINCT
          t2.file_id,
          t2.ref_id,

          t2.amount_transaction,
          t2.currency_code_transaction AS currency_transaction,

          r.rate_currency,
          try_cast(r.rate_variable AS DOUBLE) AS rate_variable,
          try_cast(r.rate_fixed AS DOUBLE)    AS rate_fixed,
          try_cast(r.rate_min AS DOUBLE)      AS rate_min,
          try_cast(r.rate_cap AS DOUBLE)      AS rate_cap,

          CASE
            WHEN r.rate_variable IS NULL THEN try_cast(r.rate_fixed AS DOUBLE)
            WHEN try_cast(r.rate_variable AS DOUBLE) IS NULL THEN NULL
            ELSE
              CASE
                WHEN (try_cast(r.rate_variable AS DOUBLE) *
                      (t2.amount_transaction *
                        CASE
                          WHEN r.rate_currency IS NULL THEN 1
                          WHEN upper(r.rate_currency) = upper(t2.txn_currency_alpha) THEN 1
                          ELSE ex.exchange_value
                        END
                      )
                    ) + COALESCE(try_cast(r.rate_fixed AS DOUBLE),0) <= try_cast(r.rate_min AS DOUBLE)
                THEN try_cast(r.rate_min AS DOUBLE)

                WHEN (try_cast(r.rate_variable AS DOUBLE) *
                      (t2.amount_transaction *
                        CASE
                          WHEN r.rate_currency IS NULL THEN 1
                          WHEN upper(r.rate_currency) = upper(t2.txn_currency_alpha) THEN 1
                          ELSE ex.exchange_value
                        END
                      )
                    ) + COALESCE(try_cast(r.rate_fixed AS DOUBLE),0) >= try_cast(r.rate_cap AS DOUBLE)
                THEN try_cast(r.rate_cap AS DOUBLE)

                ELSE
                  (try_cast(r.rate_variable AS DOUBLE) *
                    (t2.amount_transaction *
                      CASE
                        WHEN r.rate_currency IS NULL THEN 1
                        WHEN upper(r.rate_currency) = upper(t2.txn_currency_alpha) THEN 1
                        ELSE ex.exchange_value
                      END
                    )
                  ) + COALESCE(try_cast(r.rate_fixed AS DOUBLE),0)
              END
          END AS fee,

          a.intelica_id,
          a.region_country_code,
          a.ird

        FROM t2
        INNER JOIN a
          ON try_cast(t2.ref_id AS BIGINT) = try_cast(a.ref_id AS BIGINT)
         AND t2.file_id = a.file_id

        LEFT JOIN r
          ON try_cast(r.intelica_id AS BIGINT) = try_cast(a.intelica_id AS BIGINT)
         AND upper(r.region_country_code) = upper(a.region_country_code)
         AND upper(r.ird) = upper(a.ird)
         AND {where_valid}

        LEFT JOIN ex
          ON CAST(ex.rate_date AS DATE) = CAST(t2.txn_date AS DATE)
         AND try_cast(ex.currency_from_code AS BIGINT) = try_cast(t2.currency_code_transaction AS BIGINT)
         AND upper(ex.currency_to) = upper(r.rate_currency)
        """

        df_fee = con.execute(sql).df()
        log.logger.debug(f"[calculate_interchange_fee_duckdb_legacy] rows={len(df_fee)}")
        return df_fee

    finally:
        con.close()
