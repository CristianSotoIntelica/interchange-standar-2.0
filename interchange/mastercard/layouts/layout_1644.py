from __future__ import annotations
from typing import Dict, Union
from functools import lru_cache
import pandas as pd

PdsLayout = Dict[str, Union[int, Dict[str, int]]]

DICT_PDS_LYT_1644: PdsLayout = {

    #INICIO FUNCTION CODE: 691
    
    "PDS_5": {
        "PDS_5_1": 3,
        "PDS_5_2": 5,
        "PDS_5_3": 4,
        "PDS_5_4": 3,
    },
    "PDS_6": 10,
    "PDS_25":7,
    "PDS_138":8,
    "PDS_280":25,
     #FIN FUNCTION CODE: 691
     
    "PDS_58": 100,   
    "PDS_59": 100,   
    "PDS_137": 20,  
    "PDS_148": 60,   
    #"PDS_191": 1,   
    "PDS_300": 25,  
    "PDS_302": 1,   
    "PDS_368": 2,   
    "PDS_369": 6,    
    "PDS_374": 2,    
    "PDS_378": 1,   
    "PDS_400": 10,   
    "PDS_401": 10,   
    "PDS_402": 10,  

    "PDS_165": {
        "PDS_165_1": 1,   # 1-1
        "PDS_165_2": 29,  # 2-30
    },
  
    "PDS_358": {
        "PDS_358_1": 3,   
        "PDS_358_2": 1,  
        "PDS_358_3": 6,  
        "PDS_358_4": 2,   
        "PDS_358_5": 6,  
        "PDS_358_6": 2, 
        "PDS_358_7": 1,  
        "PDS_358_8": 3,  
        "PDS_358_9": 1,  
        "PDS_358_10": 1,  
        "PDS_358_11": 1,  
        "PDS_358_12": 1, 
        "PDS_358_13": 1,   
    },

    "PDS_359": {
        "PDS_359_1": 11, 
        "PDS_359_2": 28,  
        "PDS_359_3": 1,   
        "PDS_359_4": 10,  
        "PDS_359_5": 1, 
        "PDS_359_6": 6,  
        "PDS_359_7": 2,   
        "PDS_359_8": 6, 
        "PDS_359_9": 2, 
    },

    "PDS_370": {
        "PDS_370_1": 19, 
        "PDS_370_2": 19,  
    },

    "PDS_372": {
        "PDS_372_1": 4,   
        "PDS_372_2": 3,  
    },

    "PDS_380": {
        "PDS_380_1": 1,   
        "PDS_380_2": 16,  
    },

    "PDS_381": {
        "PDS_381_1": 1,
        "PDS_381_2": 16,
    },

    "PDS_384": {
        "PDS_384_1": 1,
        "PDS_384_2": 16,
    },

    "PDS_390": {
        "PDS_390_1": 1,
        "PDS_390_2": 16,
    },

    "PDS_391": {
        "PDS_391_1": 1,
        "PDS_391_2": 16,
    },

    "PDS_392": {
        "PDS_392_1": 2,   
        "PDS_392_2": 1,   
        "PDS_392_3": 15, 
    },

    "PDS_393": {
        "PDS_393_1": 2,
        "PDS_393_2": 1,
        "PDS_393_3": 15,
    },

    "PDS_394": {
        "PDS_394_1": 1,
        "PDS_394_2": 16,
    },

    "PDS_395": {
        "PDS_395_1": 1,
        "PDS_395_2": 15,  
    },

    "PDS_396": {
        "PDS_396_1": 1,
        "PDS_396_2": 16,
    },

    # "PDS_149": {
    #     "PDS_149_1": 3,  
    #     "PDS_149_2": 3,  
    # },

    # "PDS_158": {
    #     "PDS_158_1": 3,  
    #     "PDS_158_2": 1,   # 4-4
    #     "PDS_158_3": 6,   # 5-10
    #     "PDS_158_4": 2,   # 11-12
    #     "PDS_158_5": 6,   # 13-18
    #     "PDS_158_6": 2,   # 19-20
    #     "PDS_158_7": 1,   # 21-21
    #     "PDS_158_8": 5,   # 22-26 ( OJO que se pisa con 25-25 y 26-26)
    #     "PDS_158_9": 1,   # 25-25
    #     "PDS_158_10": 1,   # 26-26
    #     "PDS_158_11": 1,   # 27-27
    #     "PDS_158_12": 1,   # 28-28
    #     "PDS_158_13": 1,   # 29-29
    # },

    # "PDS_159": {
    #     "PDS_159_1": 11,  # 1-11
    #     "PDS_159_2": 28,  # 12-39
    #     "PDS_159_3": 1,   # 40-40
    #     "PDS_159_4": 10,  # 41-50
    #     "PDS_159_5": 1,   # 51-51
    #     "PDS_159_6": 6,   # 52-57
    #     "PDS_159_7": 2,   # 58-59
    #     "PDS_159_8": 6,   # 60-65
    #     "PDS_159_9": 2,   # 66-67
    # },
}

DICT_DE_LYT_1644: Dict[str, Union[int, Dict[str, int]]] = {
    "DE_25": 4,
    "DE_26": 4,
    "DE_48": 999,
    "DE_50": 3,
    "DE_51": 3,
}

TUPLE_DE_PDS_LYT_1644 = ("DE_48",)

BASE_COLS_1644 = [
    "FILE_IDN",
    "FILE_DT",
    "MSG_NO",
    "BLOCK",
    "MTI",
    "ENC",
    "FUNCTION_CODE",
    "FUNCTION_ROLE",
    "PARSE_OK",
    "DE_1",
]

BASE_COLS_1644_EXTRACT = [
    "FILE_IDN", 
    "FILE_DT", 
    "MSG_NO", 
    "FUNCTION_CODE", 
]

RENAME_COLS_1644 = {
    "MSG_NO": "ref_id",
}

 # Tags PDS qie se esperan por Function Code en MTI 1644
PDS_TAGS_BY_FUNCTION_CODE_1644: dict[str, set[int]] = {
    "685": {148, 165, 300, 302, 358, 370, 372, 374, 378, 380, 381, 384, 390, 391, 392, 393, 394, 395, 396, 400, 401, 402},
    "688": {148, 300, 302, 359, 368, 369, 370, 372, 374, 378, 380, 381, 384, 390, 391, 392, 393, 394, 395, 396, 400, 401, 402},
    '691': {5, 6, 25, 138, 165, 280}
}


def wanted_pds_tags_1644(function_code: str) -> set[int]:
    """
    Devuelve los tags PDS (int) que se deben extraer del DE48 según function_code.
    Si no está definido, por defecto extrae TODOS los tags del layout (modo legacy).
    """
    fc = str(function_code) if function_code is not None else ""

    tags = PDS_TAGS_BY_FUNCTION_CODE_1644.get(fc)
    if tags:
        #print(tags) devuelve todos los pds que se van usar de acuerdo al function code
        return tags 
    

    # fallback: todos los tags definidos en el layout
    out: set[int] = set()
    for k in DICT_PDS_LYT_1644.keys():
        # k = "PDS_358"
        if k.startswith("PDS_"):
            out.add(int(k.split("_")[1]))
            
    return out


def pds_layout_1644_for_tags(tags: set[int]) -> PdsLayout:
    """
    Filtra DICT_PDS_LYT_1644 y devuelve solo PDS_* cuyos tags están en `tags`.
    Esto sirve para expandir subfields solo cuando aplica.
    """
    out: PdsLayout = {}
    for k, spec in DICT_PDS_LYT_1644.items():
        if not k.startswith("PDS_"):
            continue
        tag = int(k.split("_")[1])
        if tag in tags:
            out[k] = spec
    return out


def pds_layout_1644_for_function_code(function_code: str) -> PdsLayout:
    """
    Layout reducido para expandir subfields según FC.
    """
    tags = wanted_pds_tags_1644(function_code)
    return pds_layout_1644_for_tags(tags)


DE_COLS_BY_FUNCTION_CODE_1644: dict[str, list[str]] = {
    "685": ["DE_25", "DE_26", "DE_50", "DE_51"],
    "688": ["DE_25", "DE_26", "DE_50", "DE_51"],
    "691": [], 
}

# fallback si el FC no está definido
DE_COLS_DEFAULT_1644 = ["DE_25", "DE_26", "DE_50", "DE_51"]

#tags para PDS (sin subfields), por FC
PDS_FORCE_RAW_BY_FC_1644: dict[str, set[int]] = {
    "685": {400, 401, 402, 148, 300, 302, 374, 378},
    "688": {148, 368, 369, 300, 302, 374, 378, 400, 401, 402},
    "691": set(),
}

@lru_cache(maxsize=64)
def output_columns_1644_for_fc(function_code: str) -> tuple[str, ...]:
    fc = str(function_code) if function_code is not None else ""

    # 1) DEs por FC
    de_cols = DE_COLS_BY_FUNCTION_CODE_1644.get(fc, DE_COLS_DEFAULT_1644)

    # 2) tags PDS por FC (si no existe, puedes usar wanted_pds_tags_1644(fc) si quieres fallback legacy)
    tags = PDS_TAGS_BY_FUNCTION_CODE_1644.get(fc, set())
    force_raw = PDS_FORCE_RAW_BY_FC_1644.get(fc, set())

    cols: list[str] = list(de_cols)

    # 3) columnas PDS según layout: dict => subfields; int => top-level
    for tag in sorted(tags):
        key = f"PDS_{tag}"
        spec = DICT_PDS_LYT_1644.get(key)
        if spec is None:
            continue

        # si lo fuerzas a raw => solo PDS_<tag>
        if tag in force_raw:
            cols.append(key)
            continue

        if isinstance(spec, dict):
            cols.extend(spec.keys())
        else:
            cols.append(key)

    # dedup manteniendo orden
    seen = set()
    cols = [c for c in cols if not (c in seen or seen.add(c))]
    return tuple(cols)


def extract_df_1644_by_fc(df: pd.DataFrame, function_code: str, *, keep_base: bool = True) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    # 1) columnas esperadas según FC
    wanted = list(output_columns_1644_for_fc(function_code))    

    # 2) agrega columnas base primero si aplica
    if keep_base:
        wanted = [c for c in BASE_COLS_1644_EXTRACT if c in df.columns] + wanted

    # 3) crea las columnas faltantes con NA (parche)
    missing = [c for c in wanted if c not in df.columns]
    if missing:
        df = df.copy()  # evita SettingWithCopy y side-effects en el caller
        for c in missing:
            df[c] = pd.NA
    
    # 4) devuelve solo en el orden esperado
    return df.loc[:, wanted]

def normalize_columns_1644(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Renombra columnas semánticas (ej: MSG_NO -> ref_id)
    - Convierte todos los nombres a minúscula
    """
    if df is None or df.empty:
        return df

    df = df.rename(columns=RENAME_COLS_1644)
    df.columns = [c.lower() for c in df.columns]
    return df