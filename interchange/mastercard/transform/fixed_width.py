from __future__ import annotations

import pandas as pd

def expand_fixed_width_series_to_df(
    serie: pd.Series, spec: dict[str, int], *, prefix: str | None = None,
) -> pd.DataFrame:
    """
    serie: serie of text
    spec: specifications of cuts
    prefix (optional): prefix of columns (ex: 'DE_'3)

    Return: Dataframe with new columns
    """

    if serie is None or len(serie) == 0:
        return pd.DataFrame(index=getattr(serie, "index", None))
    
    # Normalize string
    s = serie.fillna("").astype(str)

    mask = s.ne("")
    if not mask.any():
        cols = [f"{prefix}{k}" if prefix else k for k in spec.keys()]
        return pd.DataFrame({c: pd.NA for c in cols}, index=serie.index)
    
    s_cut = s.where(mask) # Los vacions vuelven NAN
    
    out: dict[str, pd.Series] = {}
    pos = 0 
    for name, ln in spec.items():
        col_name = f"{prefix}{name}" if prefix else name
        # slice vectorizado pero en filas vacias quedara en NaN
        out[col_name] = s_cut.str.slice(pos, pos + int(ln))
        pos = pos + int(ln)

    df_out = pd.DataFrame(out, index=serie.index)

    return df_out.where(df_out.notna(), pd.NA)


def expand_fixed_width_columns(
        df: pd.DataFrame, specs_by_col: dict[str, dict[str, int]],
        *,
        only_if_present: bool = True,
) -> pd.DataFrame:
    """
    Expands multiples columns fixed-witdh (each col with spec) and do an only concat
    df: dataframe base
    specs_by_col: {"DE_3": {"DE_3_1": 2, ...}, "PDS_146": {...} }
    only_if_present: if True, ignore cols that not exists in the df.
    
    Return: df_out = df + new columns
    """

    if df is None or df.empty or not specs_by_col:
        return df
    
    parts: list[pd.DataFrame] = []

    for col, spec in specs_by_col.items():
        # si permite ignorar y el col del specs no está en el df = no está en el df
        if only_if_present and col not in df.columns: 
            continue

        s = df[col]
        # Si esta  vacio al 100% no se expande
        non_empty = s.notna() & (s.astype(str).str.len() > 0)
        if not non_empty.any():
            continue

        sub_df = expand_fixed_width_series_to_df(serie=s, spec=spec)
        parts.append(sub_df)

    if not parts:
        return df
    
    sub_all = pd.concat(parts, axis=1)
    return pd.concat([df, sub_all], axis=1)
