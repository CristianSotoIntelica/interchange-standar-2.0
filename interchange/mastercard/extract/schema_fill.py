from __future__ import annotations

import numpy as np
import pandas as pd 
from interchange.persistence.database import Database 

def normalize_extract_name(name: str) -> str:
    """
    Normalize an extract column name to the same convention used in the pipeline.

    Parameters
    ----------
    name: str
        Raw extract name from metadata (may contain spaces / mixed casting).

    Returns
    -------
    str
        Normalized column name (lowercase, trimmed, spaces replaced with "_").
    """
    return str(name).strip().lower().replace(" ", "_")

def fill_mising_from_db(
        df: pd.DataFrame,
        db: Database,
        missing_tokens: list[str],
        *,
        table_name: str = "de_pds_extract_names",
) -> pd.DataFrame:
    """
    Add missing layout columns to a DataFrame using DB metadata.

    Given a list of missing DE/PDS layout tokens (e.g. "de_25", "pds_358_1"), 
    this function looks up the corresponding 'extract_name' in the metadata DB 
    and adds the normalized column to the DataFrame if it does not exists.

    Parameters
    ----------
    df: pandas.DataFrame 
        Input DataFrame (already renamed/normalized by the extraction pipeline).
    db: Database
        Database connection used to resolve tokens to extract column names.
    missing_tokens: list[str]
        Missing layout tokens detectd by schema validation (lowercase tokens).
    table_name: str, optional
        Metadata table name. Defaults to "de_pds_extract_names".

    Returns
    -------
    pandas.DataFrame
        Copy of the DataFrame with missing extract columns added.

    Notes
    -----
    - New columns are created with 'np.nan' values (NULL-like).
    - Tokens not found in the metadata table are skipped silently.
    """
    df_out = df.copy()

    for token in missing_tokens:
        parts = token.split("_")
        tlv_field = parts[0].upper() # "DE" / "PDS"
        tag = parts[1]
        subfield = parts[2] if len(parts) == 3 else "0" 

        # Resolve token -> extrac_name via metadata table
        df_db = db.read_records(
            table_name=table_name,
            fields= ["tlv_field", "tag", "extract_name"],
            where={"tlv_field": tlv_field, "tag": tag, "subfield": subfield},
        )

        if df_db.empty:
            continue

        extract_name = normalize_extract_name(df_db.iloc[0]["extract_name"])

        # Add the missing column if not present
        if extract_name not in df_out.columns:
            df_out[extract_name] = pd.NA

    return df_out