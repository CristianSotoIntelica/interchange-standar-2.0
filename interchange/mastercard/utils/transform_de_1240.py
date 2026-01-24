import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq

DICT_DE_LYT_1240 = {
    "DE_2": 19,
    "DE_3": {"DE_3_1": 2, "DE_3_2": 2, "DE_3_3": 2},
    "DE_4": 14,
    "DE_5": 14,
    "DE_6": 14,
    "DE_9": 8,
    "DE_10": 8,
    "DE_12": {"DE_12_1": 6, "DE_12_2": 6},
    "DE_14": 4,
    "DE_22": {"DE_22_1": 1, "DE_22_2": 1, "DE_22_3": 1, "DE_22_4": 1, 
              "DE_22_5": 1, "DE_22_6": 1, "DE_22_7": 1, "DE_22_8": 1, 
              "DE_22_9": 1, "DE_22_10": 1, "DE_22_11": 1, "DE_22_12": 1},
    "DE_23": 3,
    "DE_24": 3,
    "DE_25": 4,
    "DE_26": 4,
    "DE_30": {"DE_30_1": 12, "DE_30_2": 12},
    "DE_31": {"DE_31_1": 1, "DE_31_2": 6, "DE_31_3": 4, "DE_31_4": 11, 
              "DE_31_5": 1},
    "DE_32": 11,
    "DE_33": 11,
    "DE_37": 12,
    "DE_38": 6,
    "DE_40": 3,
    "DE_41": 8,
    "DE_42": 15,
    "DE_43": {"DE_43_1": 90, "DE_43_2": 90, "DE_43_3": 90, "DE_43_4": 90, 
              "DE_43_5": 90, "DE_43_6": 90},
    "DE_48": 999,
    "DE_49": 3,
    "DE_50": 3,
    "DE_51": 3,
    "DE_54": 120,
    "DE_62": 999,
    "DE_63": 16,
    "DE_71": 8,
    "DE_72": 999,
    "DE_73": 6,
    "DE_93": 11,
    "DE_94": 11,
    "DE_95": 10,
    "DE_100": 11,
    "DE_111": 12,
    "DE_123": 100,
    "DE_124": 100,
    "DE_125": 100,
    "DE_127": 100
}

BASE_COLS_1240 = ["MSG_NO", "BLOCK", "MTI", "ENC", "FUNCTION_CODE", "FUNCTION_ROLE", 
                  "PARSE_OK", "DE_1"]


def filter_df_columns_de(
        client_id: str, file_id: str, df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.rename(columns=str.upper)
    cols_to_keep = ([c for c in BASE_COLS_1240 if c in df.columns] 
    + [c for c in DICT_DE_LYT_1240.keys() if c in df.columns])

    df_de_only = df[cols_to_keep]

    return df_de_only
    
def split_fixed_width(value: str, spec: dict[str, int]) -> dict[str, str]:
    
    if value is None:
        value = ""

    value = str(value)
    
    out = {}
    pos = 0

    for name, ln in spec.items():
        out[name] = value[pos: pos+ln]
        pos = pos + ln
    return out

def expand_subfields(df: pd.DataFrame, layout: dict) -> pd.DataFrame:
    
    df_out = df.copy()

    for de_name, de_spec in layout.items():
        # solo si existe la columna y tiene subcampos (dict)
        if de_name not in df_out.columns:
            continue
        if not isinstance(de_spec, dict):
            continue

        # split por fila y expandir a columnas
        expanded = df_out[de_name].apply(lambda x: split_fixed_width(x, de_spec)).apply(pd.Series)

        # (opcional) prefijo/orden: ya vienen como DE_3_1, DE_3_2...
        # Unir al df
        df_out = df_out.join(expanded)

    return df_out