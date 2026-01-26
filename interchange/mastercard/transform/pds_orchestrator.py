from __future__ import annotations

from typing import Iterable, Dict, Union, cast
from interchange.mastercard.transform.fixed_width import expand_fixed_width_columns
from interchange.mastercard.layouts.layout_1240 import (
    DICT_PDS_LYT_1240, TUPLE_DE_PDS_LYT_1240
)

import pandas as pd

PdsLayout = Dict[str, Union[int, Dict[str, int]]]

def parse_pds_tlv_scan(blob: str, wanted_tags: set[int]) -> dict[str, str]:
    """
    Parse PDS TLV with format:
    T ag (4 digits) + L en (3 digits) + V alue (LEN chars)

    Rules:
    - If in the position dont appear a valid TLV: jump +1 char and continue
    - If TLV is valid but TAG dont appear in wanted_tags: jump +len chars
    - If TLV is valid and TAG appear in wanted_tags: save the Value
    
    Return: 
    {"PDS_<tag>": "<value>, ...}
    """

    if blob is None:
        blob = ""

    blob = str(blob)

    out: dict[str, str] = {}
    i = 0
    n = len(blob)

    while i + 7 <= n:
        tag_txt = blob[i:i+4]
        len_txt = blob[i+4:i+7]

        # Verified if TLV valid appear in this position
        if not (tag_txt.isdigit() and len_txt.isdigit()):
            i = i + 1
            continue
        
        tag = int(tag_txt)
        ln = int(len_txt)

        # Verified if length is valid or not ( jump +1 char)
        if ln < 0 or ln > 999:
            i = i + 1
            continue

        start_val = i + 7
        end_val = start_val + ln

        # Verified if end TLV es valid or not (jump +1 char)
        if end_val > n:
            i = i + 1 
        
        # TLV valid
        if tag in wanted_tags:
            out[f"PDS_{tag}"] = blob[start_val:end_val]

        # Success saved TLV: Jump TLV lengh
        i = end_val

    return out

def extract_pds_columns_from_containers(
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
    
    container_cols = list(container_cols)
    # Get the list of cols that in your df have the container_cols
    present_cols = [c for c in container_cols if c in df.columns]

    if not present_cols:
        return df

    # Parse per row: produces dict {"PDS_2": "...", "PDS_146": "..."}
    def parse_row(row: pd.Series) -> dict[str, str]:
        out: dict[str, str] = {}
        
        for c in present_cols:
            # Get the body of the row of the col that appear in the present_cols
            blob = row.get(c)

            if blob is None or blob == "":
                continue

            out.update(parse_pds_tlv_scan(blob=blob, wanted_tags=wanted_tags))

        return out

    pds_df = df.apply(parse_row, axis=1).apply(pd.Series)

    # Secure that all columns exists (include null)
    expected_cols = [f"PDS_{t}" for t in sorted(wanted_tags)]

    for c in expected_cols:
        if c not in pds_df.columns:
            pds_df[c] = pd.NA

    return pd.concat([df, pds_df[expected_cols]], axis=1)

def extract_pds_columns_from_containers_fast(
        df: pd.DataFrame, *, container_cols: Iterable[str], wanted_tags: set[int],
) -> pd.DataFrame:
    """
    Extract PDS TLV from one o many containers columns (DE_xxx) 
    and create columns PDS_<tag>

    - container_cols: columns where can appear TLV (DE_48, DE62, ...)
    - wanted_trags: set of numerics tags of PDS that we need extract ({2, 3, 146, ...})
    """
    if df is None or df.empty or not wanted_tags:
        return df
    
    container_cols = list(container_cols)
    present_cols = [c for c in container_cols if c in df.columns]
    if not present_cols:
        return df
    
    # 1) Parse each container like Serie of dicts {PDS_x: value}
    dict_series_list: list[pd.Series] = []

    for c in present_cols:
        serie = df[c].fillna("").astype(str)

        # Each blob (each row of this column) is parse and put in dict_series
        dict_series = serie.map(lambda blob: parse_pds_tlv_scan(
            blob=blob, wanted_tags=wanted_tags) if blob else {})
        
        #Append the dic_series in dict_series_list (a list of dictionaries)
        dict_series_list.append(dict_series)
        
    # 2) Combined dicts in order
    combined = dict_series_list[0]

    for nxt in dict_series_list[1:]:
        combined = combined.combine(nxt, lambda a, b: (a or {}) | (b or {}))

    # 3) Expand dicts to columns 
    pds_df = combined.apply(pd.Series)

    # 4) Secure that have all wanted columns in order
    expected_cols = [f"PDS_{t}" for t in sorted(wanted_tags)]
    for col in expected_cols:
        if col not in pds_df.columns:
            pds_df[col] = pd.NA

    return pd.concat([df, pds_df[expected_cols]], axis=1)

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

def apply_pds_for_mti_1240(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline PDS for MTI 1240:
    1) extract PDS from containers (DE_48, DE_62, DE_123, DE_124, DE_125)
    2) expands subfields for the PDSs that required (DICT_PDS_LYT_1240)
    """

    # Tags to extract: all PDS of the Layout (PDS_2, PDS_146, etc)
    wanted_tag = {int(k.split("_")[1]) for k in DICT_PDS_LYT_1240.keys()}

    df2 = extract_pds_columns_from_containers_fast(
        df=df, container_cols=TUPLE_DE_PDS_LYT_1240, wanted_tags=wanted_tag)
    
    df3 = expand_pds_subfields(df=df2, pds_layout=DICT_PDS_LYT_1240)

    return df3