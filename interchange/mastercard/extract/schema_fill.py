from __future__ import annotations

import numpy as np
import pandas as pd 
from interchange.persistence.database import Database 

def normalize_extract_name(name: str) -> str:
    return str(name).strip().lower().replace(" ", "_")

def fill_mising_from_db(
        df: pd.DataFrame,
        db: Database,
        missing_tokens: list[str],
        *,
        table_name: str = "de_pds_extract_names",
) -> pd.DataFrame:
    df_out = df.copy()

    for token in missing_tokens:
        parts = token.split("_")
        tlv_field = parts[0].upper()
        tag = parts[1]
        subfield = parts[2] if len(parts) == 3 else "0" 

        df_db = db.read_records(
            table_name=table_name,
            fields= ["tlv_field", "tag", "extract_name"],
            where={"tlv_field": tlv_field, "tag": tag, "subfield": subfield},
        )

        if df_db.empty:
            continue

        extract_name = normalize_extract_name(df_db.iloc[0]["extract_name"])

        if extract_name not in df_out.columns:
            df_out[extract_name] = np.nan

    return df_out