from __future__ import annotations
from interchange.persistence.database import Database
import pandas as pd

def base_clean_param() -> pd.DataFrame:
    """
    Define the mandatory "base" columns for Mastercard clean layer.

    These columns are expected to exist (or be created) in the cleaned output across MTIs,
    even if they are not present in the DB-driven metadata table.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the same structure as `de_pds_extract_names` metadata:
        - extract_name
        - data_type
    """
    return pd.DataFrame(
        [
            {"extract_name": "file_idn", "data_type": "string"},
            {"extract_name": "file_dt", "data_type": "string"},
            {"extract_name": "type_mti", "data_type": "string"},
            {"extract_name": "ref_id", "data_type": "int64"},
            {"extract_name": "function_code", "data_type": "int64"},
        ]
    )

def extend_field_defs_with_base_cols(field_defs: pd.DataFrame) -> pd.DataFrame:
    """
    Extend DB field definitions with required base columns.

    This helper ensures that clean-layer casting always includes a minimal set of common columns
    (file_idn, file_dt, type_mti, ref_id, function_code), regardless of whether they appear in
    the DB metadata table.

    Rules
    -----
    - Base columns are prepended (appear first in the resulting definitions).
    - If duplicates exist (same extract_name), base definitions win.

    Parameters
    ----------
    field_defs : pandas.DataFrame
        DataFrame loaded from DB containing dtype rules.

    Returns
    -------
    pandas.DataFrame
        Combined and de-duplicated definitions suitable for metadata-driven casting.
    """
    base_defs = base_clean_param()

    # Prepend base columns to guarantee they exist and appear first.
    out = pd.concat([base_defs, field_defs], ignore_index=True)

    # If duplicates exist, keep the first occurrence (base_defs has priority).
    out["extract_name"] = out["extract_name"].astype(str).str.strip()
    out = out.drop_duplicates(subset=["extract_name"], keep="first")

    return out

def load_mc_field_dtype_definitions() -> pd.DataFrame:
    """
    Load Mastercard dtype definitions used for metadata-driven casting.

    The returned DataFrame is consumed by:
    - interchange.mastercard.clean.fields_dtype_def.cast_df_from_params_def
    - interchange.mastercard.clean.fields_dtype_def.build_arrow_schema_from_params

    Expected DB table
    -----------------
    de_pds_extract_names with columns:
    - extract_name : str
        Target column name in the cleaned dataset.
    - data_type : str
        One of {"int64", "string", "decimal", "timestamp", "date", "time"}.
    - float_decimals : int | null
        Decimal scale rule (used when data_type == "decimal"):
        - >= 0      : implied decimals scale
        - -1        : scale-prefixed decimals (conversion rates)
        - -2/-3/-4  : dynamic implied decimals by currency code (DE49/DE50/DE51)

    Returns
    -------
    pandas.DataFrame
        Normalized metadata with:
        - extract_name stripped
        - data_type stripped + lowercased
        - float_decimals as pandas nullable Int64 (if present)

    Side effects
    ------------
    Reads from the database via Database.read_records().

    Notes
    -----
    - This function only normalizes raw metadata; validation of allowed types is handled downstream.
    - Column ordering for parquet output is enforced by the clean step (after casting),
      then propagated into the Arrow schema via ordered_cols.
    """
    db = Database()
    fd = db.read_records(
        table_name="de_pds_extract_names",
        fields=["extract_name", "data_type", "float_decimals"],
    )

    fd = fd.copy()
    fd["extract_name"] = fd["extract_name"].astype(str).str.strip()
    fd["data_type"] = fd["data_type"].astype(str).str.strip().str.lower()

    if "float_decimals" in fd.columns:
        fd["float_decimals"] = pd.to_numeric(fd["float_decimals"], errors="coerce").astype("Int64")

    return fd