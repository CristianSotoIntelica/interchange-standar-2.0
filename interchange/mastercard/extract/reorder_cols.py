from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd 

from interchange.persistence.database import Database
from interchange.mastercard.extract.nomalize import normalize_col

def build_ordered_extract_names_from_layout_keys(
        db: Database, 
        layout_keys: Iterable[str], 
        *, 
        table_name: str = "de_pds_extract_names"
) -> list[str]:
    """
    Build an ordered list of normalized extract column names from layout keys.

    This helper converts layout keys (e.g. "DE_25", "PDS_358_1") into the 
    corresponding extract names defined in the metadata database, preserving 
    the input order of 'layout_keys'.

    Parameters 
    -----------
    db: Database 
        Database connection usted to read metadata.
    layout_keys: Iterable[str]
        Layout keys in the form "DE_<tag>[_<subfield>]" or "PDS_<tah>[_<subfield>]".
    table_name: str, optional
        Metada table name. Defaults to "de_pds_extract_names".

    Returns
    -------
    list[str]
        Ordered list of normalized extract column names (deduplicated),
        bases on the provided layout keys. Keys not found in the metadata are skipped.
    """
    wanted: list[tuple[str, str, str]] = []

    # Convert layout keys into (TLV_FIELD, TAG, SUBFIELD) tuples for lookup.
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
    
    # Normalize metadata fields for stable matching.
    df_cat["tlv_field"] = df_cat["tlv_field"].astype(str).str.upper().str.strip()
    df_cat["tag"] = df_cat["tag"].astype(str).str.strip()
    df_cat["subfield"] = df_cat["subfield"].astype(str).str.strip()
    df_cat["extract_name"] = df_cat["extract_name"].astype(str)

    # Build mapping (TLV_FIELD, TAG, SUBFIELD) -> normalized extract_name
    mapping: dict[tuple[str, str, str], str] = {}

    for _, r in df_cat.iterrows():
        key = (r["tlv_field"], r["tag"], r["subfield"])
        mapping[key] = normalize_col(r["extract_name"])

    ordered: list[str] = []
    for key in wanted:
        name = mapping.get(key)
        if name:
            ordered.append(name)

    # Deduplicate while preserving order.
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
    """
    Reorder DataFrame columns into: first_cols -> layout_cols -> extras.

    Parameters
    ----------
    df: pandas.DataFrame
        Input DataFrame.
    ordered_layout_cols: Iterable[str]
        Expected layout column names (usually produced by 
        'build_ordered_extract_names_from_layout_keys').
    first_cols: Iterable[str] | None, optional
        Columns that must appear first if present (e.g. msg_no, mti, parse_ok).

    Returns
    -------
    pandas.DataFrame
        A copy of the DataFrame with columns reordered.

    Notes
    -----
    - Column names are normalzied via 'nomalize_col' before reordering.
    - Columns not listed in 'first_cols' or 'ordered_layout_cols' are preserved
    and appended at the end (extras).
    """
    out = df.copy()

    # Normalize column names for consistent matching.
    out.columns = [normalize_col(c) for c in out.columns]
    cols = list(out.columns)

    first_cols_n = [normalize_col(c) for c in (first_cols or [])]
    layout_cols_n = [normalize_col(c) for c in ordered_layout_cols]

    # 1) Keep first_cols in the requested order (only those present).
    first = [c for c in first_cols_n if c in cols]

    # 2) Add layout columns in layout order, skipping duplicates / already used.
    layout = []
    used = set(first)
    for c in layout_cols_n:
        if c in cols and c not in used:
            layout.append(c)
            used.add(c)

    # 3) Preserve any remaining columns at the end (extras).
    extras = [c for c in cols if c not in used]

    return out[first + layout + extras]