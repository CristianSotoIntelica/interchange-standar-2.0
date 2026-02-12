from __future__ import annotations
import pandas as pd 
from interchange.mastercard.calculate.layout_calculate_fields import (
    AMOUNT_COLS_FINAL, EXRATE_COLS_FINAL, PRE2_COLS_FINAL, KEYS
)
from typing import Mapping, Sequence, Any, Optional
from decimal import Decimal, InvalidOperation
import pyarrow as pa

def _ensure_required_cols(df: pd.DataFrame, required: list[str], df_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{df_name}] faltan columna requeridas: {missing}")
    
def _dedupe_on_keys(
        df: pd.DataFrame, 
        *, 
        df_name: str, 
        keys: list[str] = KEYS,
        strategy: str = "error", # "error" | "first"
) -> pd.DataFrame:
    """Garantiza 1 fila por key. Si duplicates: error o colapsa por first"""
    dup_mask = df.duplicated(subset=keys, keep=False)
    if dup_mask.any():
        if strategy == "error":
            sample = df.loc[dup_mask, keys].head(10)
            raise ValueError(
                f"[{df_name}] hay duplicados por {keys}. Ejemplos:\n{sample.to_string(index=False)}"
            )
        if strategy == "first":
            return df.drop_duplicates(subset=keys, keep="first")
        raise ValueError(f"[{df_name}] strategy inválida: {strategy}")
    return df

def build_mc_calculated_df(
        *,
        df_pre2: pd.DataFrame,
        df_ex_rate: pd.DataFrame,
        df_amount: pd.DataFrame,
        dedupe_strategy: str = "error", # "error" | "fist"
):
    """
    Unifica DF_PRE_2 + DF_EX_RATE + DF_AMOUNT por (file_id, ref_id) y devuelve DF final
    con las columnas especificadas.

    Regla:
    - DF_PRE_2 puede venir con múltiples filas por key => se filtra N==1 si existe.
    - Joins:
        final = pre2 (base) LEFT JOIN ex_rate LEFT JOIN amount
    """

    # --- PRE2 ---
    _ensure_required_cols(df=df_pre2, required=["file_id", "ref_id"], df_name="DF_PRE_2")
    pre2 = df_pre2.copy()

    # Si existe n, nos quedamos con n == 1
    if "n" in pre2.columns:
        pre2 = pre2[pre2["n"].astype("Int64") == 1].copy()

    # Asegurar columnas finales
    _ensure_required_cols(df=pre2, required=PRE2_COLS_FINAL, df_name="DE_PRE_2 (para tabla final)")
    pre2 = pre2[PRE2_COLS_FINAL].copy()
    pre2 = _dedupe_on_keys(df=pre2, df_name="DF_PRE_2", strategy=dedupe_strategy)

    # --- EX_RATE ---
    _ensure_required_cols(df=df_ex_rate, required=["file_id", "ref_id"], df_name="DF_EX_RATE")
    _ensure_required_cols(df=df_ex_rate, required=EXRATE_COLS_FINAL, df_name="DF_EX_RATE (para tabla final)")
    exr = df_ex_rate[EXRATE_COLS_FINAL].copy()
    exr = _dedupe_on_keys(df=exr, df_name="DF_EX_RATE", strategy=dedupe_strategy)

    # --- AMOUNT ---
    _ensure_required_cols(df=df_amount, required=["file_id", "ref_id"], df_name="DF_AMOUNT")
    _ensure_required_cols(df=df_amount, required=AMOUNT_COLS_FINAL, df_name="DF_AMOUNT (para tabla final)")
    amt = df_amount[AMOUNT_COLS_FINAL].copy()
    amt = _dedupe_on_keys(df=amt, df_name="DF_AMOUNT", strategy=dedupe_strategy)

    # --- MERGES (LEFT JOIN para no perder base) ---
    final = pre2.merge(exr, on=KEYS, how="left", validate="one_to_one")
    final = final.merge(amt, on=KEYS, how="left", validate="one_to_one")

    # ordenar columnas
    ordered_cols = KEYS + [c for c in final.columns if c not in KEYS]
    final = final[ordered_cols]

    return final

def decimal_from_value(x: Any):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x 
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none", "<na>"):
        return None
    
    try:
        return Decimal(s)
    except(InvalidOperation, ValueError):
        return None
    
def to_decimal_series(s: pd.Series) -> pd.Series:
    return s.map(decimal_from_value)

def to_int64(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype("string").str.strip(), errors="coerce").astype("Int64")

def to_str(s: pd.Series) -> pd.Series:
    return s.astype("string")

def to_ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def to_date(s: pd.Series) -> pd.Series:
    # date32 funciona bien con objetos datetime.date
    return pd.to_datetime(s, errors="coerce").dt.date

def cast_df_from_layout(df: pd.DataFrame, layout: Mapping[str, Any]) -> pd.DataFrame:
    out = df.copy()

    for col, spec in layout.items():
        if col not in out.columns:
            continue

        dtype = spec.strip().lower() if isinstance(spec, str) else str(spec.get("dtype", "")).strip().lower()

        if dtype in ("int64", "bigint", "int"):
            out[col] = to_int64(out[col])
        elif dtype in ("string", "str", "varchar", "text"):
            out[col] = to_str(out[col])
        elif dtype in ("timestamp", "datetime"):
            out[col] = to_ts(out[col])
        elif dtype in ("date", "date32"):
            out[col] = to_date(out[col])
        elif dtype in ("decimal", "numeric"):
            out[col] = to_decimal_series(out[col])

    return out