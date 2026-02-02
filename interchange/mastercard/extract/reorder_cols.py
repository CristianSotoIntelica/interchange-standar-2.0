from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd 

from interchange.persistence.database import Database
from interchange.mastercard.extract.nomalize import normalize_col

def build_ordered_extract_names_from_layout_keys(
        db: Database, layout_keys: Iterable[str], *, 
        table_name: str = "de_pds_extract_names"
) -> list[str]:
    
    wanted: list[tuple[str, str, str]] = []

    for k in layout_keys:
        parts = str(k).split("_")
        if len(parts) < 2:
            continue # invalid key
        tlv_field = parts[0].upper() # DE / PDS
        tag = parts[1]
        subfield = parts[2] if len(parts) > 2 else "0"
        wanted.append((tlv_field, str(tag), str(subfield)))

    if not wanted:
        return []
    
    df_cat = db.read_records(
        table_name=table_name, 
        fields=["tlv_field", "tag", "subfield", "extract_name"],
        where={}
    )

    if df_cat.empty:
        return []
    
    df_cat["tlv_field"] = df_cat["tlv_field"].astype(str).str.upper().str.strip()
    df_cat["tag"] = df_cat["tag"].astype(str).str.strip()
    df_cat["subfield"] = df_cat["subfield"].astype(str).str.strip()
    df_cat["extract_name"] = df_cat["extract_name"].astype(str)

    mapping: dict[tuple[str, str, str], str] = {}

    for _, r in df_cat.iterrows():
        key = (r["tlv_field"], r["tag"], r["subfield"])
        mapping[key] = normalize_col(r["extract_name"])

    ordered: list[str] = []
    for key in wanted:
        name = mapping.get(key)
        if name:
            ordered.append(name)

    seen: set[str] = set()
    out: list[str] = []
    for c in ordered:
        if c not in seen:
            out.append(c)
            seen.add(c)

    return out 

def reorder_df_columns(
        df: pd.DataFrame,
        ordered_layout_cols: Iterable[str],
        *,
        first_cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    
    out = df.copy()

    out.columns = [normalize_col(c) for c in out.columns]
    cols = list(out.columns)

    first_cols_n = [normalize_col(c) for c in (first_cols or [])]
    layout_cols_n = [normalize_col(c) for c in ordered_layout_cols]

    first = [c for c in first_cols_n if c in cols]

    layout = []
    used = set(first)
    for c in layout_cols_n:
        if c in cols and c not in used:
            layout.append(c)
            used.add(c)

    extras = [c for c in cols if c not in used]

    return out[first + layout + extras]