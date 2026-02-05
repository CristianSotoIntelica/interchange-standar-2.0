from __future__ import annotations
from interchange.persistence.database import Database
import pandas as pd

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
        - >= 0  : implied decimals scale
        - -1    : scale-prefixed decimals (conversion rates)
        - -2/-3/-4 : dynamic implied decimals by currency code (DE49/DE50/DE51)

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
    This function does not validate that `data_type` values are within the allowed
    set; validation is handled by downstream casting logic.
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