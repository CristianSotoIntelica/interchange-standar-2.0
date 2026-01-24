import pandas as pd
from pathlib import Path
import pyarrow.parquet as pq
from typing import Dict, Union, cast

PdsLayout = Dict[str, Union[int, Dict[str, int]]]

DICT_PDS_LYT_1240: PdsLayout = {
    "PDS_2": 3,
    "PDS_3": 3,
    "PDS_23": 3,
    "PDS_25": 7,
    "PDS_146": {
        "PDS_146_1": 2,
        "PDS_146_2": 2,
        "PDS_146_3": 2,
        "PDS_146_4": 3,
        "PDS_146_5": 12,
        "PDS_146_6": 3,
        "PDS_146_7": 12,
    },
    "PDS_148": {
        "PDS_148_1": 3,
        "PDS_148_2": 1,
    },
    "PDS_158": {
        "PDS_158_1": 3,
        "PDS_158_2": 1,
        "PDS_158_3": 6,
        "PDS_158_4": 2,
        "PDS_158_5": 6,
        "PDS_158_6": 2,
        "PDS_158_7": 1,
        "PDS_158_8": 3,
        "PDS_158_9": 1,
        "PDS_158_10": 1,
        "PDS_158_11": 1,
        "PDS_158_12": 1,
        "PDS_158_13": 1,
    },
    "PDS_165": {
        "PDS_165_1": 1,
        "PDS_165_2": 29,
    },
}

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

TUPLE_DE_PDS_LYT_1240 = ("DE_48", "DE_62", "DE_123", "DE_124", "DE_125")

BASE_COLS_1240 = ["MSG_NO", "BLOCK", "MTI", "ENC", "FUNCTION_CODE", "FUNCTION_ROLE", 
                  "PARSE_OK", "DE_1"]

# pdsLayout = 


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

    out: dict[str, str] = {}
    pos = 0
    for name, ln in spec.items():
        out[name] = value[pos:pos+ln]
        pos += ln
    return out

def expand_one_fixed_width(df: pd.DataFrame, col:str, spec: dict[str, int]) -> pd.DataFrame:
    s = df[col].fillna("").astype(str)

    pos = 0 
    for name, ln in spec.items():
        df[name] = s.str.slice(pos, pos + ln)
        pos = pos + ln
    return df

def expand_subfields(df: pd.DataFrame, mti: str) -> pd.DataFrame:
    
    if mti == '1240':
        dic_layout = DICT_DE_LYT_1240
    else:
        raise ValueError(f"Unsupported mti_layout: {mti}")
    
    df_out = df.copy()

    for de_name, de_spec in dic_layout.items():
        # solo si existe la columna y tiene subcampos (dict)
        if de_name not in df_out.columns:
            continue
        if not isinstance(de_spec, dict):
            continue

        # split por fila y expandir a columnas
        df_out = expand_one_fixed_width(df=df_out, col=de_name, spec=de_spec)

    return df_out

def reorder_with_subfield(df: pd.DataFrame, mti_layout: str) -> pd.DataFrame:

    if mti_layout == '1240':
        layout = DICT_DE_LYT_1240
    else: 
        raise ValueError(f"Unsupported mti_layout: {mti_layout}")

    col_set = set(df.columns)
    cols = []

    for c in df.columns:
        cols.append(c)
        spec = layout.get(c)
        if isinstance(spec, dict):
            # Agregar subcampos si existen
            for subc in spec.keys():
                if subc in col_set:
                    cols.append(subc)

    # quitar duplicados manteniendo orden
    cols = list(dict.fromkeys(cols))
    
    return df[cols]

def extract_pds(body_de: str, find_pds: int) -> str:

    # Validar si los 4 primeros caracteres son numericos
    if body_de[0:4].isnumeric():
        len_body_de = len(body_de)
        len_pds = int(body_de[4:7]) 
        tag_id_pds = int(body_de[0:4])
        len_max_pds = len_pds + 7 # 7 = 4 (tag_id_pds) + 3 (len_pds)

        if tag_id_pds == find_pds:
            return body_de[7: len_max_pds]
        elif len_max_pds < len_body_de:
            return extract_pds(body_de=body_de[len_max_pds:], find_pds=find_pds)
        else:
            return ''
    else:
        return ''
    
#######################################################################################
# PDS #
#######################################################################################

# Ordenar PDS
def build_pds_columns_order(pds_layout: PdsLayout) -> list[str]:
    cols: list[str] = []

    for pds_name, spec in pds_layout.items():
        cols.append(pds_name)

        if isinstance(spec, dict):
            cols.extend(list(spec.keys()))
    return cols

# Para expandir subcampos de PDS con subfields
def expand_pds_subfields(df: pd.DataFrame, pds_layout: PdsLayout) -> pd.DataFrame:
    df_out = df

    for pds_name, spec in pds_layout.items():
        if not isinstance(spec, dict):
            continue

        spec_dict = cast(dict[str, int], spec)

        if pds_name not in df_out.columns:
            continue

        sub_df = df_out[pds_name].apply(lambda x: split_fixed_width(value=x, spec=spec)).apply(pd.Series)

        for subc in spec.keys():
            if subc not in sub_df.columns:
                sub_df[subc] = pd.NA

        df_out = df_out.join(sub_df)

    return df_out

def parse_pds_tlv_scan(
        blob: str, wanted_tag: set[int]) -> dict[str, str]:
    """
    Parsea PDS TLV con formato:
    TAG (4 digitos) + LEN (3 digitos) + VALUE (LEN chars)

    - Si en una posicion hay TAG/LEN númerico válidos pero no buscado, saltar completo.
    - Si no hay TLV valido: avanzar 1 char (basura) y seguir.
    """

    if blob is None:
        blob = ""

    blob = str(blob)

    out: dict[str, str] = {}
    i = 0
    n = len(blob) # length del DE

    while i + 7 <= n:
        tag_txt = blob[i:i+4]
        len_txt = blob[i+4:i+7]

        # 1) si no parece TLV (tag no valido, len no valido), avanzar 1
        if not(tag_txt.isdigit() and len_txt.isdigit()):
            i = i + 1
            continue
        
        tag = int(tag_txt) # Tag ID del PDS
        ln = int(len_txt) # Length del body del PDS

        # 2) si el valor del len es mayor al maximo, avanzar 1 
        if ln < 0 or ln > 999:
            i = i + 1 
            continue

        start_val = i + 7 # Inicio del body del PDS
        end_val = start_val + ln # Fin del body del PDS

        # 3) TLV inconsistente: length del PDS es mayor al length DE (blob), avanzar 1
        if end_val > n:
            i = i + 1 
            continue
        
        # 4) TLV Valido!
        if tag in wanted_tag:
            out[f"PDS_{tag}"] = blob[start_val:end_val]

        # 5) Saltar todo el TLV (sea valido o un TLV que no es el buscado)
        i = end_val

    return out