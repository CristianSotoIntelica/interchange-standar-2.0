from __future__ import annotations

import re 
import pandas as pd 

#Precompiled regex to collapse one or more whitespace characters
_WS_RE = re.compile(r"\s+")

def normalize_col(name: object) -> str:
    """
    Normalize a single column name for consistent schema comparison
    """
    s = str(name).strip().lower()
    s = _WS_RE.sub("_", s)
    return s

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame column names

    Parameters
    ----------
    df: pandas.Dataframe

    Returns
    -------
    pandas.Dataframe 
        Copy of the Dataframe with normalized column names.
    """
    out = df.copy()
    out.columns = [normalize_col(c) for c in out.columns]
    return out