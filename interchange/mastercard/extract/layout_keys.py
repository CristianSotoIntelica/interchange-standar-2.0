from typing import Any, Dict, List
from itertools import chain

def collect_all_keys(layout: Dict[str, Any]) -> List[str]:
    """"
    Collect all dictionary keys from a nested layout definition.

    Parameters
    ----------
    layout: dict[str, Any]
        Layout dictionary where keys represent DE/PDS tokens and values can be 
        either an int (fixed length) or another dict (subfields).

    Returns
    ----------
    list[str]
        Depth-first list of keys as they appear while walking the dictionary. 
        Includes both top-level keys and nested subfield keys.
    """
    keys: List[str] = []

    def _walk(d: Dict[str, Any]) -> None:
        for k, v in d.items():
            keys.append(k)
            if isinstance(v, dict):
                _walk(v)
    _walk(layout)
    return keys

def build_expected_keys(*layouts: Dict[str, Any]) -> List[str]:
    """
    Build a unique, ordered list of expected layout keys from one or more layouts. 
    This function flatten
    """
    all_lists = [collect_all_keys(d) for d in layouts]
    return list(dict.fromkeys(chain.from_iterable(all_lists)))