from typing import Any, Dict, List
from itertools import chain

def collect_all_keys(layout: Dict[str, Any]) -> List[str]:
    keys: List[str] = []

    def _walk(d: Dict[str, Any]) -> None:
        for k, v in d.items():
            keys.append(k)
            if isinstance(v, dict):
                _walk(v)
    _walk(layout)
    return keys

def build_expected_keys(*layouts: Dict[str, Any]) -> List[str]:
    all_lists = [collect_all_keys(d) for d in layouts]
    return list(dict.fromkeys(chain.from_iterable(all_lists)))