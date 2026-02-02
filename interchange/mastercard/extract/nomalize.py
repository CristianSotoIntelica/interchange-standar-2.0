from __future__ import annotations

import re 
import pandas as pd 

_WS_RE = re.compile(r"\s+")

def normalize_col(name: object) -> str:
    s = str(name).strip().lower()
    s = _WS_RE.sub("_", s)
    return s

def normalize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_col(c) for c in out.columns]
    return out