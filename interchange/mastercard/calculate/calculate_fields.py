from __future__ import annotations
import pandas as pd 
from interchange.mastercard.calculate.layout_calculate_fields import (
    AMOUNT_COLS_FINAL, EXRATE_COLS_FINAL, PRE2_COLS_FINAL, KEYS
)
from typing import Mapping, Sequence, Any
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
    
def build_arrow_schema_from_layout(
    *,
    layout: Mapping[str, Any],
    ordered_cols: Sequence[str],
    default_decimal_precision: int = 18,
    default_decimal_scale: int = 4,
    timestamp_unit: str = "ns",
) -> pa.Schema:
    """
    Construye un pa.Schema para la capa CAL desde un layout dict.

    layout soporta:
      - col -> "string" | "int64" | "timestamp" | "date" | "decimal"
      - col -> {"dtype":"decimal", "precision":18, "scale":4}
        (precision/scale opcionales; toman defaults si faltan)
    """

    def _arrow_type(col: str, spec: Any) -> pa.DataType:
        # Spec puede ser str o dict
        if isinstance(spec, str):
            t = spec.strip().lower()

            if t in ("string", "str", "varchar", "text"):
                return pa.string()
            if t in ("int64", "bigint", "int"):
                return pa.int64()
            if t in ("timestamp", "datetime"):
                return pa.timestamp(timestamp_unit)
            if t in ("date", "date32"):
                return pa.date32()
            if t in ("decimal", "numeric"):
                return pa.decimal128(default_decimal_precision, default_decimal_scale)

            raise ValueError(f"Tipo no soportado en layout: col={col!r}, type={t!r}")

        if isinstance(spec, dict):
            dtype = str(spec.get("dtype", "")).strip().lower()

            # Permitimos que el usuario use "decimal" o incluso "numeric"
            if dtype in ("decimal", "numeric"):
                prec = int(spec.get("precision", default_decimal_precision))
                scale = int(spec.get("scale", default_decimal_scale))

                # Validaciones básicas
                if prec <= 0:
                    raise ValueError(f"[{col}] precision inválida: {prec}")
                if scale < 0:
                    raise ValueError(f"[{col}] scale inválido: {scale}")
                if scale > prec:
                    raise ValueError(f"[{col}] scale ({scale}) no puede ser > precision ({prec})")

                return pa.decimal128(prec, scale)

            # Si en el futuro quieres soportar dict para timestamp, etc.
            # podrías extender aquí.
            raise ValueError(f"Dict spec no soportado para col={col!r}: {spec}")

        raise ValueError(f"Spec inválido para col={col!r}: {spec!r} (esperado str o dict)")

    fields: list[pa.Field] = []
    for col in ordered_cols:
        if col not in layout:
            raise ValueError(f"Columna {col!r} no está definida en layout")
        pa_type = _arrow_type(col, layout[col])
        fields.append(pa.field(col, pa_type, nullable=True))

    return pa.schema(fields)