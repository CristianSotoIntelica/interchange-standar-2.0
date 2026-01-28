from __future__ import annotations

from typing import Iterable, Dict, Union, cast
import pandas as pd

from interchange.mastercard.transform.fixed_width import expand_fixed_width_columns
from interchange.mastercard.layouts.layout_1240 import (DICT_PDS_LYT_1240, TUPLE_DE_PDS_LYT_1240)
from interchange.mastercard.layouts.layout_1644 import (DICT_PDS_LYT_1644, TUPLE_DE_PDS_LYT_1644,pds_layout_1644_for_function_code, wanted_pds_tags_1644, pds_layout_1644_for_tags )
from interchange.mastercard.layouts.layout_1740 import (DICT_PDS_LYT_1740, TUPLE_DE_PDS_LYT_1740)

PdsLayout = Dict[str, Union[int, Dict[str, int]]]

def get_pds_layout_by_mti(mti: str):
    if mti == "1240":
        return DICT_PDS_LYT_1240, TUPLE_DE_PDS_LYT_1240
    elif mti == "1644":
        return DICT_PDS_LYT_1644, TUPLE_DE_PDS_LYT_1644
    elif mti == "1740":
        return DICT_PDS_LYT_1740, TUPLE_DE_PDS_LYT_1740
    else:
        raise ValueError(f"Unsupported MTI for PDS pipeline: {mti}")
    
def parse_pds_tlv_scan_txt(blob: str, wanted_tag_txt: set[str]) -> dict[str, str]:
    """
    Parse PDS TLV with format:
    T ag (4 digits) + L en (3 digits) + V alue (LEN chars)

    Rules:
    - If in the position dont appear a valid TLV: jump +1 char and continue
    - If TLV is valid but TAG dont appear in wanted_tag_txt: jump +len chars
    - If TLV is valid and TAG appear in wanted_tag_txt: save the Value
    
    Return: 
    {"PDS_<tag>": "<value>, ...}
    """

    if not blob:
        return {}
    
    n = len(blob)
    out: dict[str, str] = {}

    i = 0
    while i + 7 <= n:
        tag_txt = blob[i:i+4]
        len_txt = blob[i+4:i+7]

        # Verified if TLV valid appear in this position
        if not (tag_txt.isdigit() and len_txt.isdigit()):
            i = i + 1
            continue
        
        ln = int(len_txt)

        # Verified if length is valid or not ( jump +1 char)
        if ln > 999:
            i = i + 1
            continue

        start_val = i + 7
        end_val = start_val + ln

        # Verified if end TLV es valid or not (jump +1 char)
        if end_val > n:
            i = i + 1 
            continue
        
        # TLV valid
        if tag_txt in wanted_tag_txt:
            out[f"PDS_{int(tag_txt)}"] = blob[start_val:end_val]

        # Success saved TLV: Jump TLV lengh
        i = end_val

    return out

def extract_pds_columns_from_containers_fast(
        df: pd.DataFrame, *, container_cols: Iterable[str], wanted_tags: set[int],
) -> pd.DataFrame:
    """
    Extract PDS TLV from one o many containers columns (DE_xxx) 
    and create columns PDS_<tag>

    - container_cols: columns where can appear TLV (DE_48, DE62, ...)
    - wanted_trags: set of numerics tags of PDS that we need extract ({2, 3, 146, ...})
    """
    # Verified if df is empty or if wanted_tags is empty
    if df is None or df.empty or not wanted_tags:
        return df
    
    wanted_tag_txt = {f"{t:04d}" for t in wanted_tags}

    # Get the list of cols that in your df have the container_cols
    present_cols = [c for c in container_cols if c in df.columns]
    if not present_cols:
        return df
    
    n = len(df)
    
    # Filter containers with real data
    cols_with_data: list[str] = []
    series_cache: dict[str, pd.Series] = {}

    for c in present_cols:
        s = df[c].fillna("").astype(str)
        series_cache[c] = s
        non_empty = (s != "").sum()
        print(f"[PDS] {c}: non-empty {non_empty}/{n} ({non_empty/n:.1%})")
        if non_empty > 0:
            cols_with_data.append(c)

    if not cols_with_data:
        return df

    # 2) Parser a list 
    parsed_per_col: list[list[dict[str, str]]] = []

    for c in cols_with_data:
        s = series_cache[c]
        blobs = s.to_numpy(dtype=object)

        # list comprehension 
        parsed = [
            parse_pds_tlv_scan_txt(blob=b, wanted_tag_txt=wanted_tag_txt) if b else {}
            for b in blobs
        ]

        parsed_per_col.append(parsed)

    # 3) Merge per row only if have more than 1 container with data
    if len(parsed_per_col) == 1:
        combined = parsed_per_col[0]
    else:
        combined: list[dict[str, str]] = [{} for _ in range(n)]
        for i in range(n):
            d: dict[str, str] = {}
            for col_list in parsed_per_col:
                if col_list[i]:
                    d.update(col_list[i])
            combined[i] = d

    # 4) Expands dicts to columns
    expected_cols = [f"PDS_{t}" for t in sorted(wanted_tags)]
    pds_df = pd.DataFrame.from_records(combined)
    pds_df.index = df.index
    pds_df = pds_df.reindex(columns=expected_cols)
    pds_df = pds_df.where(pds_df.notna(), pd.NA)

    return pd.concat([df, pds_df], axis=1)

def expand_pds_subfields(
        df: pd.DataFrame, *, pds_layout: PdsLayout) -> pd.DataFrame:
    """
    Expand subfields of PDS when the layout defined like dictionary.
    """
    if df is None or df.empty:
        return df
    
    # mapping: col -> spec (only when have dict and exists in the df)
    mapping: dict[str, dict[str, int]] = {}

    for pds_name, spec in pds_layout.items():
        if not isinstance(spec,dict):
            continue
        
        if pds_name not in df.columns:
            continue
        
        mapping[pds_name] = cast(dict[str, int], spec)

    if not mapping:
        return df
        
    return expand_fixed_width_columns(df, mapping)


def wanted_tags_from_layout(pds_layout: dict) -> set[int]:
    return {int(k.split("_")[1]) for k in pds_layout.keys()}


def apply_pds_for_mti(df: pd.DataFrame, *, mti: str) -> pd.DataFrame:
    """
    Pipeline PDS para MTI 1240, 1644 y 1740:
    1) extrae PDS desde contenedores definidos por el layout (DE_48, DE_62, etc.)
    2) expande subfields para PDS que lo requieran (los que en layout son dict)
    """
    if df is None or df.empty:
        return df

    # normalizar columnas a UPPER
    if any(c != c.upper() for c in df.columns):
        df = df.copy()
        df.columns = [c.upper() for c in df.columns]

    pds_layout, container_cols = get_pds_layout_by_mti(mti)
    wanted_tags = wanted_tags_from_layout(pds_layout)

    df2 = extract_pds_columns_from_containers_fast(
        df=df,
        container_cols=container_cols,
        wanted_tags=wanted_tags,
    )

    df3 = expand_pds_subfields(
        df=df2,
        pds_layout=pds_layout,
    )

    return df3


def apply_pds_for_mti_1644_split(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Devuelve {'685': df_685, '688': df_688} ya con PDS extraídos/expandidos.
    Si no hay filas para un FC, no lo incluye en el dict.
    """
    if df is None or df.empty:
        return {}

    # normalizar columnas a UPPER
    if any(c != c.upper() for c in df.columns):
        df = df.copy()
        df.columns = [c.upper() for c in df.columns]

    if "FUNCTION_CODE" not in df.columns:
        return {}

    fc_series = df["FUNCTION_CODE"].astype(str)
    df = df[fc_series.isin({"685", "688","691"})]
    if df.empty:
        return {}

    out: dict[str, pd.DataFrame] = {}

    for fc, g in df.groupby("FUNCTION_CODE", dropna=False):
        fc_str = str(fc)

        tags = wanted_pds_tags_1644(fc_str)
        pds_layout_fc = pds_layout_1644_for_tags(tags)

        g2 = extract_pds_columns_from_containers_fast(
            df=g,
            container_cols=TUPLE_DE_PDS_LYT_1644,  # DE_48
            wanted_tags=tags,
        )

        g3 = expand_pds_subfields(df=g2, pds_layout=pds_layout_fc)

        out[fc_str] = g3.sort_index()

    return out