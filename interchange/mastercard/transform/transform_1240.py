from __future__ import annotations

import pandas as pd
from typing import Dict, Union, cast

from interchange.mastercard.layouts.layout_1240 import (
    PdsLayout,
    DICT_DE_LYT_1240,
    DICT_PDS_LYT_1240,
    BASE_COLS_1240,
    TUPLE_DE_PDS_LYT_1240,
)

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
        pos = pos + ln
    return out # PDS subfields como diccionario {PDS_123_1: valor, PDS_123_2: valor}

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
    
def build_pds_columns_order(pds_layout: PdsLayout) -> list[str]:
    cols: list[str] = []

    for pds_name, spec in pds_layout.items():
        cols.append(pds_name)

        if isinstance(spec, dict):
            cols.extend(list(spec.keys()))
    return cols

def expand_pds_subfields(df: pd.DataFrame, pds_layout: PdsLayout) -> pd.DataFrame:
    df_out = df

    for pds_name, spec in pds_layout.items():
        if not isinstance(spec, dict): #Validar si tiene subfields
            continue

        spec_dict = cast(dict[str, int], spec) # Castear el spec como dict[str: int]

        if pds_name not in df_out.columns: #Validar si el pds_name esta en el df_out
            continue

        sub_df = (
            df_out[pds_name]
            .apply(lambda x: split_fixed_width(value=x, spec=spec_dict))
            .apply(pd.Series)
        )

        # Validar si los subfields del layout fueron encontrados en el sub_df
        # Si no encuentra, le pone un valor NA
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

    return out # return = {'PDS_2': 'value', 'PDS_3': 'value', 'PDS_4': 'value'}

