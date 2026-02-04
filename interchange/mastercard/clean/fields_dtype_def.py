from interchange.logs.logger import Logger

from typing import Optional
import pandas as pd
import pyarrow as pa
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

log = Logger(__name__)

def quantize_decimal(d: Decimal, scale: int) -> Decimal:
    q = Decimal(1).scaleb(-scale)
    return d.quantize(q, rounding=ROUND_HALF_UP)

def to_scale_prefixed_decimal(x, *, out_scale: Optional[int] = None) -> Optional[Decimal]:
    if x is None or x == "" or pd.isna(x):
        return None
    
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    s = s.replace(" ", "")

    if "." in s:
        try:
            d = Decimal(s)
        except (InvalidOperation, ValueError):
            return None
        return quantize_decimal(d, out_scale) if out_scale is not None else d
    
    if not s.isdigit() or len(s) < 2:
        return None
    
    exp = int(s[0])
    mantissa = s[1:]

    try:
        d = Decimal(mantissa).scaleb(-exp)
    except (InvalidOperation, ValueError):
        return None
    
    if out_scale is not None:
        d = quantize_decimal(d, out_scale)

    return d

def to_implied_decimal(x, scale: int) -> Optional[Decimal]:
    """
    Convierte un valor que viene como dígitos (sin punto decimal) a Decimal aplicando implied decimals:
      scale=2  -> divide entre 10^2 (=100)
      scale=3  -> divide entre 10^3 (=1000)
      scale=0  -> no divide

    Nota: si el valor ya trae punto (ej "12.34"), se deja tal cual para evitar doble-escala.
          Si quieres modo estricto (nunca viene con punto), quita esa condición.
    """
    if x is None or x == "" or pd.isna(x):
        return None

    s = str(x).strip()
    try:
        d = Decimal(s)
    except (InvalidOperation, ValueError):
        return None

    if "." in s:
        return d  # ya viene explícito

    if scale and scale > 0:
        d = d.scaleb(-scale)  # exacto: mueve el punto a la izquierda
    return d

def cast_df_from_params_def(
        df: pd.DataFrame,
        param: pd.DataFrame,
        *,
        date_format: str = "%Y%m%d",
        timestamp_format: Optional[str] = None,
        default_decimal_scale: int = 2,
        conversion_rate_scale: int = 9,
) -> pd.DataFrame:
    
    out = df.copy()

    cols = ["extract_name", "data_type"]
    has_scale = "float_decimals" in param.columns
    if has_scale:
        cols.append("float_decimals")

    p = param[cols].copy()
    p["extract_name"] = p["extract_name"].astype(str).str.strip()
    p["data_type"] = p["data_type"].astype(str).str.strip().str.lower()

    if has_scale:
        p["float_decimals"] = pd.to_numeric(p["float_decimals"], errors="coerce").astype("Int64")

    for _, row in p.iterrows():
        col = row["extract_name"]
        t = row["data_type"]

        if col not in out.columns:
            continue

        if t == "int64":
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

        elif t == "string":
            out[col] = out[col].astype("string")

        elif t == "timestamp":
            if timestamp_format:
                s = out[col].astype("string").str.strip()
                s = s.str.replace(r"\.0$", "", regex=True)
                s = s.str.zfill(12)
                out[col] = pd.to_datetime(s, format=timestamp_format, errors="coerce")
            else:
                out[col] = pd.to_datetime(out[col], errors="coerce")

        elif t == "date":
            s = out[col].astype("string").str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(6)
            out[col] = pd.to_datetime(s, format=date_format, errors="coerce").dt.date

        elif t =="time":
            s = out[col].astype("string").str.strip()
            s = s.str.replace(r"\.0$", "", regex=True)
            s = s.str.zfill(6)

            out[col] = (
                s.str.slice(0, 2) + ":" +
                s.str.slice(2, 4) + ":" +
                s.str.slice(4, 6)
            ).astype("string")
        
        elif t == "decimal":
            scale = default_decimal_scale
            if has_scale and pd.notna(row["float_decimals"]):
                scale = int(row["float_decimals"])

            if scale == -1:
                out[col] = out[col].astype("string")
                continue

            if scale == -2:
                src = out[col]
                converted = src.apply(lambda v: to_scale_prefixed_decimal(v, out_scale=conversion_rate_scale))

                bad = int(src.notna().sum() - pd.Series(converted).notna().sum())
                if bad > 0:
                    log.logger.warning(f"[cast_df_from_param] Columna '{col}' tuvo {bad} valores inválidos -> NULL")

                out[col] = converted
                continue

            # normal
            src = out[col]
            converted = src.apply(lambda x: to_implied_decimal(x, scale))

            bad = int(src.notna().sum() - pd.Series(converted).notna().sum())
            if bad > 0:
                log.logger.warning(f"[cast_df_from_param] Columna '{col}' tuvo {bad} valores inválidos -> NULL")
            
            out[col] = converted
        
        else:
            out[col] = out[col].astype("string")

    return out

def build_arrow_schema_from_params(
    param: pd.DataFrame,
    *,
    default_decimal_precision: int = 18,
    default_decimal_scale: int = 2,
    conversion_rate_scale: int = 9,
    timestamp_unit: str = "ns",
) -> pa.Schema:
    
    cols = ["extract_name", "data_type"]
    has_scale = "float_decimals" in param.columns
    if has_scale:
        cols.append("float_decimals")

    p = param[cols].copy()
    p["extract_name"] = p["extract_name"].astype(str).str.strip()
    p["data_type"] = p["data_type"].astype(str).str.strip().str.lower()
    if has_scale:
        p["float_decimals"] = pd.to_numeric(p["float_decimals"], errors="coerce").astype("Int64")
    
    fields: list[pa.Field] = []

    for _, row in p.iterrows():
        col = row["extract_name"]
        t = row["data_type"]

        if t == "int64":
            fields.append(pa.field(col, pa.int64()))

        elif t == "string":
            fields.append(pa.field(col, pa.string()))

        elif t == "timestamp":
            fields.append(pa.field(col, pa.timestamp(timestamp_unit)))

        elif t == "date":
            fields.append(pa.field(col, pa.date32()))
        
        elif t == "time":
            fields.append(pa.field(col, pa.string()))

        elif t == "decimal":
            scale = default_decimal_scale
            if has_scale and pd.notna(row["float_decimals"]):
                scale = int(row["float_decimals"])

            if scale == -1: 
                fields.append(pa.field(col, pa.string()))

            elif scale == -2:
                fields.append(pa.field(col, pa.decimal128(default_decimal_precision, conversion_rate_scale)))
            else:
                fields.append(pa.field(col, pa.decimal128(default_decimal_precision, scale)))

        else:
            fields.append(pa.field(col, pa.string()))

    return pa.schema(fields)