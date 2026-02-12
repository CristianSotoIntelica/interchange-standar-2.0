from __future__ import annotations

from typing import Any, Mapping, Sequence
import pyarrow as pa

def arrow_type_from_spec(
        *,
        col: str,
        spec: Any,
        default_decimal_precision: int = 18,
        default_decimal_scale: int = 4,
        timestamp_unit: str,
) -> pa.DataType:
    """
    Convierte el spec del layout (str o dict) a un pa.DataType.
    Soporta:
      - "string" | "int64" | "timestamp" | "date" | "decimal"
      - {"dtype":"decimal", "precision":18, "scale":4}
    """

    # --- spec como string ---
    if isinstance(spec, str):
        t = spec.strip().lower()

        if t in ("string", "str", "varchar", "text"):
            return pa.string()
        if t in("int64", "bigint", "int"):
            return pa.int64()
        if t in ("timestamp", "datetime"):
            return pa.timestamp(timestamp_unit)
        if t in ("date", "date32"):
            return pa.date32()
        if t in ("decimal", "numeric"):
            return pa.decimal128(default_decimal_precision, default_decimal_scale)
        
        raise ValueError(f"Tipo no soportado en layout: col={col!r}, type={t!r}")
    
    # --- spec como dict ---
    if isinstance(spec, dict):
        dtype = str(spec.get("dtype", "")).strip().lower()

        if dtype in ("decimal", "numeric"):
            prec = int(spec.get("precision", default_decimal_precision))
            scale = int(spec.get("scale", default_decimal_scale))

            if prec <= 0:
                raise ValueError(f"[{col}] precision inválida: {prec}")
            if scale < 0: 
                raise ValueError(f"[{col}] scale inválido: {scale}")
            if scale > prec:
                raise ValueError(f"[{col}] scale ({scale}) no puede ser > precision ({prec})")
            
            return pa.decimal128(prec, scale)
        
        raise ValueError(f"Dict spec no soportado para col={col!r}: {spec}")
    
    raise ValueError(f"Spec inválido para col ={col!r}: {spec!r} (esperado str o dict)")

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
    Usa ordered_cols para respetar el orden final del DF.
    """
    fields: list[pa.Field] = []

    for col in ordered_cols:
        if col not in layout:
            raise ValueError(f"Columna {col!r} no está definida en layout")
        
        pa_type = arrow_type_from_spec(
            col=col,
            spec=layout[col],
            default_decimal_precision=default_decimal_precision,
            default_decimal_scale=default_decimal_scale,
            timestamp_unit=timestamp_unit
        )
        fields.append(pa.field(col, pa_type, nullable=True))

    return pa.schema(fields)