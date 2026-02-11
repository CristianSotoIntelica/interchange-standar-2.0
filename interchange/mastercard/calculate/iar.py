from __future__ import annotations

import pandas as pd
from interchange.logs.logger import Logger
from interchange.persistence.database import Database

log = Logger(__name__)

def calculate_iar_unique(*, file_dt: str, db: Database) -> pd.DataFrame:
    """
    Step1: genera IAR unique
    usando SQLite tables:
    - mastercard_iar
    - mastercard_brand_product

    Retorna DF:
    - app_date_valid, low_key_for_range, high_key_for_range, 
    iar_country, gcms_product_identifier, funding_source, card_program_identifier
    """

    dt_raw = str(file_dt).strip()

    if dt_raw.isdigit() and len(dt_raw) == 6:
        dt = pd.to_datetime(dt_raw, format="%y%m%d", errors="coerce")
    else:
        dt = pd.to_datetime(dt_raw, errors="coerce")

    if pd.isna(dt):
        raise ValueError(f"[calculate_iar_unique] file_dt inválido: {file_dt!r}")

    file_dt_norm = dt.date().isoformat() # 'YYYY-MM-DD'

    sql = """
    WITH iar_pre_1 AS (
        SELECT
            date(a.effective_timestamp)                AS effective_date,
            date(a.app_date_valid)                     AS app_date_valid,
            date(a.app_date_end)                       AS app_date_end,

            CAST(substr(CAST(a.low_range  AS TEXT), 1, 18) AS INTEGER)  AS low_key_for_range,
            CAST(substr(CAST(a.high_range AS TEXT), 1, 18) AS INTEGER)  AS high_key_for_range,

            a.card_country_alpha                       AS iar_country,
            CAST(a.card_program_priority AS INTEGER)   AS card_program_priority,
            a.card_program_identifier                  AS card_program_identifier,

            b.gcms_product_id                          AS gcms_product_identifier,
            b.product_category                         AS funding_source

        FROM mastercard_iar a
        LEFT JOIN mastercard_brand_product b
          ON b.licensed_product_id = a.licensed_product_id
         AND b.active_inactive_code = 'A'

        WHERE a.active_inactive_code = 'A'
          AND date(a.app_date_valid) <= date(?)
    ),
    bin_pre_2 AS (
        SELECT
            a.*,
            row_number() OVER (
                PARTITION BY low_key_for_range
                ORDER BY app_date_valid DESC, card_program_priority
            ) AS rn
        FROM iar_pre_1 a
    )
    SELECT
        app_date_valid,
        low_key_for_range,
        high_key_for_range,
        iar_country,
        gcms_product_identifier,
        funding_source,
        card_program_identifier
    FROM bin_pre_2
    WHERE rn = 1
    """

    log.logger.debug(f"[calculate_iar_unique] file_dt={file_dt_norm}")
    df = db.read_sql(sql, (file_dt_norm,))

    if not df.empty:
        df["low_key_for_range"] = pd.to_numeric(df["low_key_for_range"], errors="coerce")
        df["high_key_for_range"] = pd.to_numeric(df["high_key_for_range"], errors="coerce")
        df["app_date_valid"] = pd.to_datetime(df["app_date_valid"], errors="coerce")

    return df