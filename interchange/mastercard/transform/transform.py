from __future__ import annotations

import pandas as pd

from interchange.mastercard.transform.fixed_width import expand_fixed_width_columns
from typing import Dict, Union, cast, Tuple


from interchange.mastercard.layouts.layout_1240 import (
    PdsLayout,
    DICT_DE_LYT_1240,
    DICT_PDS_LYT_1240,
    BASE_COLS_1240,
    TUPLE_DE_PDS_LYT_1240,
)

from interchange.mastercard.layouts.layout_1644 import (
    PdsLayout,
    DICT_DE_LYT_1644,
    DICT_PDS_LYT_1644,
    BASE_COLS_1644,
    TUPLE_DE_PDS_LYT_1644,
)

from interchange.mastercard.layouts.layout_1740 import (
    PdsLayout,
    DICT_DE_LYT_1740,
    DICT_PDS_LYT_1740,
    BASE_COLS_1740,
    TUPLE_DE_PDS_LYT_1740,
)

def get_layouts_by_mti(mti: str) -> tuple[dict, dict, list[str], tuple]:
    """
    Devuelve (DICT_DE_LYT, DICT_PDS_LYT, BASE_COLS, TUPLE_DE_PDS)
    para el MTI indicado.
    """
    if mti == "1240":
        return DICT_DE_LYT_1240, DICT_PDS_LYT_1240, BASE_COLS_1240, TUPLE_DE_PDS_LYT_1240
    elif mti == "1644":
        return DICT_DE_LYT_1644, DICT_PDS_LYT_1644, BASE_COLS_1644, TUPLE_DE_PDS_LYT_1644
    elif mti == "1740":
        return DICT_DE_LYT_1740, DICT_PDS_LYT_1740, BASE_COLS_1740, TUPLE_DE_PDS_LYT_1740
    else:
        raise ValueError(f"Unsupported MTI: {mti}")

def filter_df_columns_de( df: pd.DataFrame, mti: str) -> pd.DataFrame:
    df = df.rename(columns=str.upper)

    dict_de, _, base_cols, _ = get_layouts_by_mti(mti)

    cols_to_keep = (
        [c for c in base_cols if c in df.columns] +
        [c for c in dict_de.keys() if c in df.columns]
    )
    return df[cols_to_keep]

def expand_subfields(df: pd.DataFrame, mti: str) -> pd.DataFrame:
    dict_de, _, _, _ = get_layouts_by_mti(mti)

    if df is None or df.empty:
        return df
    
    # mapping only DE with subfields (dict) that exists in the df
    mapping: dict[str, dict[str, int]] = {}

    for de_name, de_spec in dict_de.items():
        if de_name not in df.columns:
            continue
        if not isinstance(de_spec, dict):
            continue

        mapping[de_name] = cast(dict[str, int], de_spec)

        if not mapping:
            return df

    return expand_fixed_width_columns(df=df, specs_by_col=mapping)

def reorder_with_subfield(df: pd.DataFrame, mti: str) -> pd.DataFrame:

    dict_de, _, _, _ = get_layouts_by_mti(mti)

    col_set = set(df.columns)
    cols = []

    for c in df.columns:
        cols.append(c)
        spec = dict_de.get(c)
        if isinstance(spec, dict):
            # Agregar subcampos si existen
            for subc in spec.keys():
                if subc in col_set:
                    cols.append(subc)

    # quitar duplicados manteniendo orden
    cols = list(dict.fromkeys(cols))
    
    return df[cols]
    