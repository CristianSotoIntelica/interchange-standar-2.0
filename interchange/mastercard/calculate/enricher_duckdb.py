from __future__ import annotations

from pathlib import Path
import pandas as pd
import duckdb

from interchange.logs.logger import Logger
from interchange.persistence.database import Database

log = Logger(__name__)

def calculate_pre2_duckdb(
        *,
        parquet_path: str | Path,
        db: Database,
        df_iar_unique: pd.DataFrame,
        client_id: str | None = None,
        file_id: str | None = None,
        country_table: str = "country",
        region_table: str = "region",
        client_table: str = "client",
) -> pd.DataFrame:
    """
    Pre2 = PASO 2 + PASO 3 + PASO 4 (sistema 1), usando DuckDB.

    Requiere:
      - parquet CLN con columnas:
          ref_id, file_id, file_type, type_mti, file_dt,
          settlement_indicator_1_pds_165_1,
          pan_de_2, acquirer_reference_data_de_31,
          date_and_time_local_transaction_de_12,
          card_acceptor_country_code_de_43_6
      - df_iar_unique (Step1) con:
          app_date_valid, low_key_for_range, high_key_for_range,
          iar_country, gcms_product_identifier, funding_source, card_program_identifier

    Devuelve df con:
      - keys: ref_id, file_id, file_type, type_mti, file_dt
      - PASO2: settlement_indicator, purchase_date, card_purchase_country, card_purchase_region,
               acquirer_bin, iss_bin_6, iss_bin_8, num_card_low, num_card_high
      - PASO3: business_mode, jurisdiction, jurisdiction_country, jurisdiction_region + campos IAR
      - PASO4: n (row_number por ref_id,file_id)
    """

    parquet_path = str(Path(parquet_path).resolve())

    # 1) Dimensiones desde SQLite (pequeñas)
    df_country = db.read_sql(
        f"""
        SELECT
            country_code_alternative,
            country_code,
            mastercard_region_code
        FROM {country_table}
        """
    )

    df_region = db.read_sql(
        f"""
        SELECT
            region_code
        FROM {region_table}
        """
    )

    df_client = db.read_sql(
        f"""
        SELECT 
        client_id,
        acquiring_bins,
        issuing_bins_6_digits,
        issuing_bins_8_digits
        FROM {client_table} 
        WHERE upper(client_id) = upper(?)
        """,
        params=(client_id,),
    )

    # 2) DuckDB
    where_sql = ""
    params: list[object] = [parquet_path]

    if file_id is not None:
        where_sql = "WHERE t.file_id = ?"
        params.append(file_id)

    con = duckdb.connect()
    try:
        con.register("iar", df_iar_unique)
        con.register("country", df_country)
        con.register("region", df_region)
        con.register("client", df_client)

        sql = f"""
        WITH t AS (
            SELECT 
            t.ref_id,
            t.file_id,
            t.file_idn, -- new field
            t.file_type,
            t.type_mti,
            t.file_dt,

            t.settlement_indicator_1_pds_165_1 AS settlement_indicator,
            t.pan_de_2 as pan,
            t.acquirer_reference_data_de_31 AS acq_ref, 
            t.date_and_time_local_transaction_de_12 AS purchase_date,
            t.card_acceptor_country_code_de_43_6 AS card_purchase_country 
            FROM read_parquet(?) t 
            {where_sql}

        ),

        base AS (
            SELECT 
            ref_id,
            file_id,
            file_idn, -- new field 
            file_type,
            type_mti,
            file_dt,
            settlement_indicator,
            purchase_date,
            card_purchase_country,

            substr(CAST(acq_ref AS VARCHAR), 2, 6) AS acquirer_bin,

            regexp_extract(CAST(pan AS VARCHAR), '^(\\d{{9}})', 1) AS pan_prefix9,

            substr(regexp_extract(CAST(pan AS VARCHAR), '^(\\d{{9}})', 1), 1, 6) AS iss_bin_6,
            substr(regexp_extract(CAST(pan AS VARCHAR), '^(\\d{{9}})', 1), 1, 8) AS iss_bin_8,

            CAST(regexp_extract(CAST(pan AS VARCHAR), '^(\\d{{9}})', 1) || repeat('0', 9) AS BIGINT) AS num_card_low,
            CAST(regexp_extract(CAST(pan AS VARCHAR), '^(\\d{{9}})', 1) || repeat('9', 9) AS BIGINT) AS num_card_high

            FROM t
        ),

        joined AS (
            SELECT 
            a.ref_id, 
            a.file_id, 
            a.file_idn, -- new field
            a.file_type, 
            a.type_mti, 
            a.file_dt, 

            a.settlement_indicator, 
            a.purchase_date, 
            a.card_purchase_country, 
            ac.mastercard_region_code AS card_purchase_region, 

            a.acquirer_bin, 
            a.iss_bin_6, 
            a.iss_bin_8, 
            a.num_card_low,
            a.num_card_high, 

            CASE 
                WHEN a.file_type = 'IN' THEN 'issuing' 
                WHEN a.file_type = 'OUT' THEN 'acquiring' 
                ELSE a.file_type 
            END AS business_mode, 

            iar.app_date_valid, 
            iar.low_key_for_range, 
            iar.high_key_for_range, 
            iar.iar_country, 
            iar.gcms_product_identifier, 
            iar.funding_source, 
            iar.card_program_identifier, 

            bc.country_code AS jurisdiction_country, 
            CAST(r.region_code AS VARCHAR) AS jurisdiction_region,

            CASE
                WHEN a.card_purchase_country = iar.iar_country THEN
                    CASE
                        WHEN a.file_type = 'IN' THEN
                            CASE
                                WHEN (
                                    list_contains(
                                        string_split(regexp_replace(coalesce(c.acquiring_bins,''), '\\\\s+', ''), ','),
                                        a.acquirer_bin
                                    )
                                    OR upper(coalesce(a.settlement_indicator,'')) = 'C'
                                ) THEN 'on-us' ELSE 'off-us'
                            END
                        WHEN a.file_type = 'OUT' THEN
                            CASE
                                WHEN (
                                    list_contains(
                                        string_split(regexp_replace(coalesce(c.issuing_bins_6_digits,''), '\\\\s+', ''), ','),
                                        a.iss_bin_6
                                    )
                                    OR list_contains(
                                        string_split(regexp_replace(coalesce(c.issuing_bins_8_digits,''), '\\\\s+', ''), ','),
                                        a.iss_bin_8
                                    )
                                    OR upper(coalesce(a.settlement_indicator,'')) = 'C'
                                ) THEN 'on-us' ELSE 'off-us'
                            END
                    END

                WHEN a.card_purchase_country <> iar.iar_country AND bc.mastercard_region_code = ac.mastercard_region_code THEN 'intraregional'

                WHEN a.card_purchase_country <> iar.iar_country AND bc.mastercard_region_code <> ac.mastercard_region_code THEN 'interregional' 

            END AS jurisdiction

            FROM base a 
            INNER JOIN iar 
            ON a.num_card_low <= iar.high_key_for_range AND a.num_card_high >= iar.low_key_for_range 

            LEFT JOIN country ac 
            ON ac.country_code_alternative = a.card_purchase_country 

            LEFT JOIN country bc 
            ON bc.country_code_alternative = iar.iar_country 

            LEFT JOIN region r 
            ON r.region_code = bc.mastercard_region_code 

            LEFT JOIN client c 
            ON TRUE
        )

        SELECT 
        *,
        row_number() OVER (
            PARTITION BY ref_id, file_id, file_idn
            ORDER BY app_date_valid DESC, high_key_for_range DESC
        ) AS n 
        FROM joined
        """

        log.logger.debug(
        f"[calculate_pre2_duckdb] customer={client_id} parquet={parquet_path} file_id={file_id or 'ALL'}"
        )

        df_pre2 = con.execute(sql, params).df()

        return df_pre2

    finally:
        con.close()


def calculate_ex_rate_duckdb(
        *,
        parquet_path: str | Path,
        db: Database,
        client_id: str,
        file_id: str | None = None,
        client_table: str = "client",
        currency_table: str = "currency",
        exchange_rate_table: str = "exchange_rate",
        brand: str = "Mastercard"
) -> pd.DataFrame:
    """
    PASO 5 (sistema 1) en DuckDB:
    - Lee parquet CLN (solo columnas necesarias)
    - Joins a client/currency/exchange_rate (SQLite)
    - Calcula exchange_value_settlement / exchange_value_local
    - Devuelve DF listo para PASO 7

    Output (por ref_id, file_id):
      ref_id, file_id, file_type, type_mti, file_dt,
      amount_reconciliation, amount_transaction,
      currency_code_transaction, currency_code_reconciliation,
      exchange_value_settlement, exchange_value_local,
      local_currency_code, local_currency_code_numeric,
      settlement_currency_code, settlement_currency_code_numeric
    """

    parquet_path = str(Path(parquet_path).resolve())

    # 1) Client
    df_client = db.read_sql(
        f"""
        SELECT 
            client_id, 
            local_currency_code, 
            settlement_currency_code
        FROM {client_table}
        WHERE upper(client_id) = upper(?) 
        """,
        params=(client_id,),
    )

    # 2) Currency
    df_curr = db.read_sql(
        f"""
        SELECT 
            currency_alphabetic_code,
            currency_numeric_code
        FROM {currency_table}
        """
    )

    # 3) Exchange rates 
    df_ex = db.read_sql(
        f"""
        SELECT 
            rate_date,
            brand,
            currency_from_code,
            currency_to,
            exchange_value
        FROM {exchange_rate_table}
        WHERE upper(brand) = upper(?) 
        """,
        params=(brand,),
    )

    # 4) DuckDB
    where_sql = ""
    params: list[object] = [parquet_path]

    if file_id is not None:
        where_sql = "WHERE t.file_id = ?"
        params.append(file_id)

    con = duckdb.connect()

    try:
        con.register("cus", df_client)
        con.register("cur", df_curr)
        con.register("ex", df_ex)

        sql = f"""
        WITH t AS (
            SELECT 
                t.ref_id,
                t.file_id,
                t.file_type,
                t.file_idn, -- new field
                t.type_mti,
                t.file_dt,

            t.amount_reconciliation_de_5 AS amount_reconciliation,
            t.amount_transaction_de_4 AS amount_transaction,
            t.currency_code_transaction_de_49 AS currency_code_transaction,
            t.currency_code_reconciliation_de_50 AS currency_code_reconciliation
            FROM read_parquet(?) t
            {where_sql}
        ),

        t2 AS (
            SELECT 
                *,
                CASE 
                    WHEN regexp_matches(CAST(file_dt AS VARCHAR), '^\\d{{6}}$')
                    THEN CAST(strptime(CAST(file_dt AS VARCHAR), '%y%m%d') AS DATE)
                    WHEN regexp_matches(CAST(file_dt AS VARCHAR), '^\\d{{8}}$')
                    THEN CAST(strptime(CAST(file_dt AS VARCHAR), '%Y%m%d') AS DATE)
                    ELSE CAST(CAST(file_dt AS VARCHAR) AS DATE) 
                END AS proc_date
            FROM t
        ),

        ex2 AS (
            SELECT 
                CASE 
                    WHEN regexp_matches(CAST(rate_date AS VARCHAR), '^\\d{{6}}$')
                    THEN CAST(strptime(CAST(rate_date AS VARCHAR), '%y%m%d') AS DATE)
                    WHEN regexp_matches(CAST(rate_date AS VARCHAR), '^\\d{{8}}$')
                    THEN CAST(strptime(CAST(rate_date AS VARCHAR), '%Y%m%d') AS DATE)
                    ELSE CAST(CAST(rate_date AS VARCHAR) AS DATE)
                END AS proc_date,
                currency_from_code,
                currency_to,
                exchange_value
            FROM ex
        )

        SELECT 
            t2.ref_id,
            t2.file_id,
            t2.file_idn, -- new field 
            t2.file_type,
            t2.type_mti,
            t2.file_dt,

            t2.amount_reconciliation,
            t2.amount_transaction,

            try_cast(t2.currency_code_transaction AS INTEGER) AS currency_code_transaction,
            try_cast(t2.currency_code_reconciliation AS INTEGER) AS currency_code_reconciliation,

            cus.local_currency_code,
            cur_loc.currency_numeric_code AS local_currency_code_numeric,

            cus.settlement_currency_code,
            cur_set.currency_numeric_code AS settlement_currency_code_numeric,

            CASE
                WHEN try_cast(t2.currency_code_transaction AS INTEGER) = try_cast(cur_set.currency_numeric_code AS INTEGER)
                THEN 1
                ELSE ex_set.exchange_value
            END AS exchange_value_settlement,

            CASE
                WHEN try_cast(t2.currency_code_transaction AS INTEGER) = try_cast(cur_loc.currency_numeric_code AS INTEGER)
                THEN 1
                ELSE ex_loc.exchange_value
            END AS exchange_value_local

        FROM t2
        LEFT JOIN cus 
        ON TRUE

        LEFT JOIN cur cur_set
        ON upper(cus.settlement_currency_code) = upper(cur_set.currency_alphabetic_code)

        LEFT JOIN cur cur_loc 
        ON upper(cus.local_currency_code) = upper(cur_loc.currency_alphabetic_code)

        LEFT JOIN ex2 ex_set
        ON ex_set.proc_date = t2.proc_date
        AND try_cast(ex_set.currency_from_code AS INTEGER) = try_cast(t2.currency_code_transaction AS INTEGER)
        AND upper(ex_set.currency_to) = upper(cus.settlement_currency_code)

        LEFT JOIN ex2 ex_loc
        ON ex_loc.proc_date = t2.proc_date
        AND try_cast(ex_loc.currency_from_code AS INTEGER) = try_cast(t2.currency_code_transaction AS INTEGER)
        AND upper(ex_loc.currency_to) = upper(cus.local_currency_code)
        """

        log.logger.debug(
            f"[calculate_ex_rate_duckdb] client={client_id} parquet={parquet_path} file_id={file_id or 'ALL'} brand={brand}"
        )

        df = con.execute(sql, params).df()

        return df
    finally:
        con.close()

def calculate_settlement_report_duckdb(
    *, 
    df_ex_rate: pd.DataFrame,
    df_pre2: pd.DataFrame,
    db: Database,
    currency_table: str = "currency", 
) -> pd.DataFrame:
    """
    PASO 7 (sistema 1) SIN FI pairing.

    Requiere:
      - df_ex_rate con (mínimo):
          ref_id, file_id, file_type, type_mti,
          amount_reconciliation, amount_transaction,
          currency_code_transaction, currency_code_reconciliation,
          exchange_value_settlement, exchange_value_local,
          local_currency_code, local_currency_code_numeric,
          settlement_currency_code
      - df_pre2 con (mínimo):
          ref_id, file_id, n, jurisdiction

    Devuelve:
      ref_id, file_id,
      settlement_report_currency_code,
      settlement_report_amount,
    """

    df_cur = db.read_sql(
        f"""
        SELECT 
            currency_numeric_code,
            currency_alphabetic_code
        FROM {currency_table}
        """
    )

    con = duckdb.connect()

    try:
        con.register("de", df_ex_rate)
        con.register("cf", df_pre2)
        con.register("cur", df_cur)

        sql = f"""
        SELECT
        de.ref_id,
        de.file_id,
        de.file_idn, -- new field
        
        CASE
            WHEN de.file_type = 'IN' THEN CAST(cur_rec.currency_alphabetic_code AS VARCHAR)
            ELSE
                CASE
                    WHEN cf.jurisdiction IN ('on-us','off-us') AND try_cast(de.currency_code_transaction AS INTEGER) = try_cast(de.local_currency_code_numeric AS INTEGER) THEN CAST(de.local_currency_code AS VARCHAR)
                    ELSE CAST(de.settlement_currency_code AS VARCHAR)
                END
        END AS settlement_report_currency_code,

        CASE
            WHEN de.file_type = 'IN' THEN try_cast(de.amount_reconciliation AS DECIMAL(18,4))
            ELSE
                CASE
                    WHEN cf.jurisdiction IN ('on-us','off-us') AND try_cast(de.currency_code_transaction AS INTEGER) = try_cast(de.local_currency_code_numeric AS INTEGER) THEN 
                        CASE
                            WHEN de.exchange_value_local IS NOT NULL THEN CAST(round(try_cast(de.amount_transaction AS DECIMAL(38,4)) * try_cast(de.exchange_value_local AS DECIMAL(38,10)), 4) AS DECIMAL(18, 4))
                        END
                    ELSE
                        CASE
                            WHEN (de.exchange_value_settlement IS NOT NULL) THEN CAST(round(try_cast(de.amount_transaction AS DECIMAL(38,4)) * try_cast(de.exchange_value_settlement AS DECIMAL(38,10)), 4) AS DECIMAL(18, 4))
                        END
                END
        END AS settlement_report_amount
        FROM de

        INNER JOIN cf
        ON cf.ref_id = de.ref_id AND cf.file_id = de.file_id AND try_cast(cf.n AS INTEGER) = 1

        LEFT JOIN cur cur_rec
        ON try_cast(cur_rec.currency_numeric_code AS INTEGER) = try_cast(de.currency_code_reconciliation AS INTEGER)

        WHERE upper(CAST(de.type_mti AS VARCHAR)) IN ('1240','1442')
        """

        log.logger.debug("[calculate_settlement_report_duckdb] step=PASO7 (sin FI)")

        return con.execute(sql).df()

    finally:
        con.close()

def calculate_calculated_fields_duckdb(
        *,
        parquet: str | Path,
        df_pre2: pd.DataFrame,
        df_amount: pd.DataFrame,
        client_id: str, 
        file_id: str | None = None,
) -> pd.DataFrame:
    """

    Ensambla:
      - base parquet (t)
      - pre2 (tp) por (ref_id, file_id) y tp.n=1
      - amount (tp1) left join por (ref_id, file_id)
    """

    parquet_path = str(Path(parquet).resolve())

    where_sql = ""
    params: list[object] = [parquet_path]

    if file_id is not None:
        where_sql = "WHERE t.file_id = ?"
        params.append(file_id)

    con = duckdb.connect()
    try:
        con.register("tp", df_pre2)
        con.register("tp1", df_amount)

        sql = f"""
        WITH t AS (
        SELECT 
            t.ref_id, 
            t.file_id, 
            t.file_type, 
            t.file_idn, -- new field
            t.type_mti, 
            t.file_dt 
        FROM read_parquet(?) t
        {where_sql}
        )
        SELECT 
            t.ref_id AS ref_id, 
            '{client_id}' AS client_id, 
            t.file_type AS file_type,
            t.file_id AS file_id, 
            t.file_idn AS file_idn, -- new field
            t.file_dt AS file_dt, 
            t.type_mti AS type_mti, 

            tp.business_mode,
            tp.jurisdiction,
            tp.jurisdiction_country, 
            tp.jurisdiction_region, 
            tp.funding_source, 
            tp.gcms_product_identifier, 
            tp.card_program_identifier, 

            CASE tp.jurisdiction 
                WHEN 'intraregional' THEN CAST(tp.jurisdiction_region AS VARCHAR)
                WHEN 'interregional' THEN '9' 
                ELSE CAST(tp.jurisdiction_country AS VARCHAR)
            END AS jurisdiction_assigned, 

            tp1.settlement_report_currency_code, 
            tp1.settlement_report_amount, 

            tp.iar_country 

        FROM t 
        INNER JOIN tp 
        ON tp.ref_id = t.ref_id AND tp.file_id = t.file_id AND try_cast(tp.n AS INTEGER) = 1
        
        LEFT JOIN tp1 
        ON tp1.ref_id = t.ref_id AND tp1.file_id = t.file_id 
        """
        log.logger.debug(
            f"[calculate_calculated_field_duckdb] parquet={parquet_path} file_id={file_id or 'ALL'}"
        )

        return con.execute(sql, params).df()
    
    finally:
        con.close()

