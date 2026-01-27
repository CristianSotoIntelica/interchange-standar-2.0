from __future__ import annotations

from typing import Dict, Union

PdsLayout = Dict[str, Union[int, Dict[str, int]]]

DICT_PDS_LYT_1740: PdsLayout = {
    "PDS_2": 3,
    "PDS_3": 3,
    "PDS_23": 3,
    "PDS_25": 7,
    "PDS_58": 100,
    "PDS_59": 100,
    "PDS_137": 20,
    "PDS_148": {
        "PDS_148_1": 3,
        "PDS_148_2": 1,
    },
    "PDS_149": {
        "PDS_149_1": 3,
        "PDS_149_2": 3,
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
    "PDS_159": {
        "PDS_159_1": 11,
        "PDS_159_2": 28,
        "PDS_159_3": 1,
        "PDS_159_4": 10,
        "PDS_159_5": 1,
        "PDS_159_6": 6,
        "PDS_159_7": 2,
        "PDS_159_8": 6,
        "PDS_159_9": 2,
    },
    "PDS_165": {
        "PDS_165_1": 1,
        "PDS_165_2": 29,
    },
    "PDS_191": 3,
}

DICT_DE_LYT_1740 = {
    "DE_2": 19,
    "DE_3": {"DE_3_1": 2},
    "DE_4": 14,
    "DE_5": 14,
    "DE_9": 8,
    "DE_24": 3,
    "DE_25": 4,
    "DE_33": 11,
    "DE_48": 999,
    "DE_49": 3,
    "DE_50": 3,
    "DE_71": 8,
}

TUPLE_DE_PDS_LYT_1740 = ("DE_48")

BASE_COLS_1740 = [
    "MSG_NO",
    "BLOCK",
    "MTI",
    "ENC",
    "FUNCTION_CODE",
    "FUNCTION_ROLE",
    "PARSE_OK",
    "DE_1",
]
