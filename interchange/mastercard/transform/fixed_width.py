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
    serie_txt = serie.fillna("").astype(str)

    out = {}
    pos = 0

    for name, ln in spec.items():
        col_name = f"{prefix}{name}" if prefix else name
        out[col_name] = serie_txt.str.slice(pos, pos + int(ln))
        pos = pos + int(ln)

    return pd.DataFrame(out, index=serie.index)

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
        
        # dividir el texto entregado segun las especificaciones
        sub_df = expand_fixed_width_series_to_df(serie= df[col], spec=spec)
        #agregar los nuevos subfields a la lista 
        parts.append(sub_df) 

    # si no hay ninguno de los specs en el df, se retorna el df puro
    if not parts:
        return df
    

    # unir como un unico dataframe las partes divididas por el specs del df
    sub_all = pd.concat(parts, axis=1)
    
    # retornar en un solo concat final: df original + nuevas columnas
    return pd.concat([df, sub_all], axis=1)

