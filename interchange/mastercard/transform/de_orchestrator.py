from __future__ import annotations

import pandas as pd

from interchange.mastercard.layouts.layout_1240 import DICT_DE_LYT_1240
from interchange.mastercard.transform.fixed_width import expand_fixed_width_columns

def expand_de_subfields(df: pd.DataFrame, *, mti: str) -> pd.DataFrame:
    """
    Expands subfields of Data Elements by layout of MTI.

    df: dataframe with columns DE_x
    mti: message type (ex: 1240)

    Return: df with subcolumns DE_x_y added.
    """

    if df is None or df.empty:
        return df
    
    # 1) Select the corresponding MTI to get Layout
    if mti == '1240':
        layout = DICT_DE_LYT_1240
    else:
        raise ValueError(f'Unsupported MTI for DE expansion {mti}')
    
    # 2) Build specs_by_col only for DEs with subfields
    specs_by_col: dict[str, dict[str, int]] = {}

    for de_name, de_spec in layout.items():
        # Verified if the de_spec is a dictionary
        if not isinstance(de_spec, dict):
            continue

        # Verified if the de_name es in the df
        if de_name not in df.columns:
            continue

        # Dictionary of specs with only DE with subfields
        specs_by_col[de_name] = de_spec

    # 3) Call the motor
    if not specs_by_col:
        return df
    
    return expand_fixed_width_columns(df=df, specs_by_col=specs_by_col)