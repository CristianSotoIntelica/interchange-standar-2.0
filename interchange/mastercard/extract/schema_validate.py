import re 
from typing import Iterable, Set, List
import pandas as pd


# Match DE_* or DE_*_* tokens (case-insensitive) not preceded by alphanumerics
RE_DE  = re.compile(r"(?<![a-z0-9])de_\d+(?:_\d+)*", re.IGNORECASE)

# Match PDS_* or PDS_*_* tokens (case-insensitive) not preceded by alphanumerics
RE_PDS = re.compile(r"(?<![a-z0-9])pds_\d+(?:_\d+)*", re.IGNORECASE)

def extract_layout_tokens_from_columns(cols: Iterable[str]) -> Set[str]:
    """
    Extract DE_* and PDS_* layout tokens from a collection of column names.

    Parameters
    ----------
    cols: Iterable[str]
        Column names to scan.

    Returns
    -------
    set[str]
        Set of normalized (lowercase) layout tokens found in the column names.
    """
    found: Set[str] = set()

    for c in cols:
        s = str(c).lower()
        found.update(m.group(0) for m in RE_DE.finditer(s))
        found.update(m.group(0) for m in RE_PDS.finditer(s))

    return found
        
def missing_layout_keys_in_parquet(
        df: pd.DataFrame, expected_keys: Iterable[str]
) -> List[str]:
    
    """
    Identify expected layout keys that are missing from a parquet DataFrame.

    This function compares the set of DE/PDS tokens found in the DataFrame 
    column names against a list of expected layout keys and returns those 
    that are not present.

    Parameters
    ----------
    df: pandas.DataFrame
        DataFrame whose column names represent extracted DE/PDS fields.
    expected_keys: Iterable[str]
        Expected DE/PDS layout keys (e.g. "de_25", "pds_358_1").

    Returns
    list[str]
        Sorted list of missing layout keys (normalized to lowercase)
    """
    tokens = extract_layout_tokens_from_columns(df.columns)
    expected = [str(k).lower() for k in expected_keys]

    return sorted(k for k in expected if k not in tokens)