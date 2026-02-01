import re 
from typing import Iterable, Set, List
import pandas as pd

RE_DE  = re.compile(r"(?<![a-z0-9])de_\d+(?:_\d+)*", re.IGNORECASE)
RE_PDS = re.compile(r"(?<![a-z0-9])pds_\d+(?:_\d+)*", re.IGNORECASE)

def extract_layout_tokens_from_columns(cols: Iterable[str]) -> Set[str]:
    found: Set[str] = set()

    for c in cols:
        s = str(c).lower()
        found.update(m.group(0) for m in RE_DE.finditer(s))
        found.update(m.group(0) for m in RE_PDS.finditer(s))

    return found
        
def missing_layout_keys_in_parquet(
        df: pd.DataFrame, expected_keys: Iterable[str]) -> List[str]:
    tokens = extract_layout_tokens_from_columns(df.columns)
    expected = [str(k).lower() for k in expected_keys]

    return sorted(k for k in expected if k not in tokens)