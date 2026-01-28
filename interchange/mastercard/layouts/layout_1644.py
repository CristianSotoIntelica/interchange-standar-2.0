from __future__ import annotations

from typing import Dict, Union

PdsLayout = Dict[str, Union[int, Dict[str, int]]]

DICT_PDS_LYT_1644: PdsLayout = {

    #INICIO FUNCTION CODE: 691
    
    "PDS_5": {
        "PDS5_1": 3,
        "PDS5_2": 5,
        "PDS5_3": 4,
        "PDS5_4": 3,
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
    "PDS_191": 1,   
    "PDS_300": 25,  
    "PDS_302": 1,   
    "PDS_368": 2,   
    "PDS_369": 6,    
    "PDS_374": 2,    
    "PDS_378": 1,   
    "PDS_400": 10,   
    "PDS_401": 10,   
    "PDS_402": 10,  

    "PDS_358": {
        "PDS358_1": 3,   
        "PDS358_2": 1,  
        "PDS358_3": 6,  
        "PDS358_4": 2,   
        "PDS358_5": 6,  
        "PDS358_6": 2, 
        "PDS358_7": 1,  
        "PDS358_8": 3,  
        "PDS358_9": 1,  
        "PDS358_10": 1,  
        "PDS358_11": 1,  
        "PDS358_12": 1, 
        "PDS358_13": 1,   
    },

    "PDS_359": {
        "PDS359_1": 11, 
        "PDS359_2": 28,  
        "PDS359_3": 1,   
        "PDS359_4": 10,  
        "PDS359_5": 1, 
        "PDS359_6": 6,  
        "PDS359_7": 2,   
        "PDS359_8": 6, 
        "PDS359_9": 2, 
    },

    "PDS_370": {
        "PDS370_1": 19, 
        "PDS370_2": 19,  
    },

    "PDS_372": {
        "PDS372_1": 4,   
        "PDS372_2": 3,  
    },

    "PDS_380": {
        "PDS380_1": 1,   
        "PDS380_2": 16,  
    },

    "PDS_381": {
        "PDS381_1": 1,
        "PDS381_2": 16,
    },

    "PDS_384": {
        "PDS384_1": 1,
        "PDS384_2": 16,
    },

    "PDS_390": {
        "PDS390_1": 1,
        "PDS390_2": 16,
    },

    "PDS_391": {
        "PDS391_1": 1,
        "PDS391_2": 16,
    },

    "PDS_392": {
        "PDS392_1": 2,   
        "PDS392_2": 1,   
        "PDS392_3": 15, 
    },

    "PDS_393": {
        "PDS393_1": 2,
        "PDS393_2": 1,
        "PDS393_3": 15,
    },

    "PDS_394": {
        "PDS394_1": 1,
        "PDS394_2": 16,
    },

    "PDS_395": {
        "PDS395_1": 1,
        "PDS395_2": 15,  
    },

    "PDS_396": {
        "PDS396_1": 1,
        "PDS396_2": 16,
    },

    "PDS_149": {
        "PDS149_1": 3,  
        "PDS149_2": 3,  
    },

    "PDS_158": {
        "PDS158_1": 3,  
        "PDS158_2": 1,   # 4-4
        "PDS158_3": 6,   # 5-10
        "PDS158_4": 2,   # 11-12
        "PDS158_5": 6,   # 13-18
        "PDS158_6": 2,   # 19-20
        "PDS158_7": 1,   # 21-21
        "PDS158_8": 5,   # 22-26 ( OJO que se pisa con 25-25 y 26-26)
        "PDS158_9": 1,   # 25-25
        "PDS158_10": 1,   # 26-26
        "PDS158_11": 1,   # 27-27
        "PDS158_12": 1,   # 28-28
        "PDS158_13": 1,   # 29-29
    },

    "PDS_159": {
        "PDS159_1": 11,  # 1-11
        "PDS159_2": 28,  # 12-39
        "PDS159_3": 1,   # 40-40
        "PDS159_4": 10,  # 41-50
        "PDS159_5": 1,   # 51-51
        "PDS159_6": 6,   # 52-57
        "PDS159_7": 2,   # 58-59
        "PDS159_8": 6,   # 60-65
        "PDS159_9": 2,   # 66-67
    },

    "PDS_165": {
        "PDS165_1": 1,   # 1-1
        "PDS165_2": 29,  # 2-30
    },
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
    "MSG_NO",
    "BLOCK",
    "MTI",
    "ENC",
    "FUNCTION_CODE",
    "FUNCTION_ROLE",
    "PARSE_OK",
    "DE_1",
]

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