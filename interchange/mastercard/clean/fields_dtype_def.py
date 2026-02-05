from interchange.logs.logger import Logger

from typing import Optional
import pandas as pd
import pyarrow as pa
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Mapping, cast, Hashable

from typing import Optional, Sequence

from interchange.persistence.database import Database

log = Logger(__name__)

def quantize_decimal(d: Decimal, scale: int) -> Decimal:
    q = Decimal(1).scaleb(-scale)
    return d.quantize(q, rounding=ROUND_HALF_UP)

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

def load_currency_decimals_map(db: Database) -> dict[str, int | None]:
    rows = db.read_records(
        table_name="currency",
        fields=["currency_numeric_code", "currency_decimal_separator"],
        where={},
    )

    if not isinstance(rows, pd.DataFrame):
        raise TypeError(f"read_records() debe devolver DataFrame aquí, pero devolvió: {type(rows)}")
    
    records: list[dict[Hashable, Any]] = rows.to_dict(orient="records")

    m: dict[str, int | None] = {}
    for r in records:
        code = str(r["currency_numeric_code"]).zfill(3)
        dec = r.get("currency_decimal_separator")
        if dec is None or str(dec).strip() == "":
            m[code] = None
        else:
            m[code] = int(dec)
    return m

def convert_dynamic_implied_amount(
    amount_str: Any,
    decimals: Any,
    *,
    default_decimals: int,
    out_scale: int,
) -> Optional[Decimal]:
    
    if amount_str is None or pd.isna(amount_str):
        return None

    s = str(amount_str).strip()
    if s == "":
        return None
    
    neg = s.startswith("-")
    if neg:
        s = s[1:]

    if not s.isdigit():
        return None

    # si no se pudo resolver decimales, usa fallback (y loggeamos fuera)
    if decimals is None or pd.isna(decimals):
        decimals = default_decimals

    try:
        d = Decimal(int(s)).scaleb(-int(decimals))
        d = quantize_decimal(d, out_scale)
        return -d if neg else d
    except (ValueError, InvalidOperation):
        return None

def cast_df_from_params_def(
        df: pd.DataFrame,
        param: pd.DataFrame,
        *,
        date_format: str = "%Y%m%d",
        timestamp_format: Optional[str] = None,
        default_decimal_scale: int = 2,
        conversion_rate_scale: int = 9,
        dynamic_decimal_out_scale: int = 4,
        currency_decimals_map: Optional[dict[str, int | None]] = None,
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

    dynamic_decimal_fields: list[tuple[str, int]] = []

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
                src = out[col]
                converted = src.apply(lambda v: to_scale_prefixed_decimal(v, out_scale=conversion_rate_scale))

                bad = int(src.notna().sum() - pd.Series(converted).notna().sum())
                if bad > 0:
                    log.logger.warning(f"[cast_df_from_param] Columna '{col}' tuvo {bad} valores inválidos -> NULL")

                out[col] = converted
                continue

            if scale in (-2, -3, -4):
                out[col] = out[col].astype("string")
                dynamic_decimal_fields.append((col, scale))
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

    if dynamic_decimal_fields:
        if currency_decimals_map is None:
            db = Database()
            currency_decimals_map = load_currency_decimals_map(db)

        scale_to_currency_col = {
            -2: "currency_code_transaction_de_49",
            -3: "currency_code_reconciliation_de_50",
            -4: "currency_code_cardholder_billing_de_51",
        }

        # col = de_4 
        # scale_flag = -2
        for col, scale_flag in dynamic_decimal_fields:
            # obtener que de usaras como moneda
            currency_col = scale_to_currency_col.get(scale_flag)
            if not currency_col or currency_col not in out.columns:
                log.logger.warning(
                    f"[cast_df_from_param] Columna '{col}' usa scale={scale_flag} pero falta '{currency_col}'. Queda NULL."
                )
                out[col] = None
                continue

            # decimales por fila, según moneda
            currency_codes_norm = (
            out[currency_col]
            .astype("string")
            .str.strip()
            .str.zfill(3)
            )

            dec_series = currency_codes_norm.map(currency_decimals_map)

            src = out[col].astype("string")

            amounts = src.tolist()
            decimals = dec_series.tolist()

            converted: list[Optional[Decimal]] = []
            for amount_str, dec in zip(amounts, decimals):
                converted.append(
                    convert_dynamic_implied_amount(
                        amount_str=amount_str,
                        decimals=dec,
                        default_decimals=default_decimal_scale,
                        out_scale=dynamic_decimal_out_scale,
                    )
                )

            bad = int(src.notna().sum() - pd.Series(converted).notna().sum())
            if bad > 0:
                log.logger.warning(f"[cast_df_from_param] Columna '{col}' tuvo {bad} valores inválidos (dinámico) -> NULL")

            if dec_series.isna().any():
                log.logger.warning(
                    f"[cast_df_from_param] Columna '{col}' tuvo monedas sin decimals en tabla currency; "
                    f"se aplicó fallback={default_decimal_scale}"
                )

            out[col] = converted        

    return out

def build_arrow_schema_from_params(
    param: pd.DataFrame,
    *,
    ordered_cols: Optional[Sequence[str]] = None,   # <- lista ordenada
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

    # Mapa rápido: col -> (type, scale)
    type_map = dict(zip(p["extract_name"], p["data_type"]))
    scale_map = dict(zip(p["extract_name"], p["float_decimals"])) if has_scale else {}

    # Si no pasas ordered_cols, usa el orden del param
    cols_out = list(ordered_cols) if ordered_cols is not None else list(p["extract_name"])

    fields: list[pa.Field] = []
    for col in cols_out:
        t = type_map.get(col, "string")  # fallback si no está en param

        if t == "int64":
            fields.append(pa.field(col, pa.int64()))
        elif t == "int32":
            fields.append(pa.field(col, pa.int32()))
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
            if has_scale and pd.notna(scale_map.get(col)):
                scale = int(scale_map[col])

            if scale == -1:
                fields.append(pa.field(col, pa.decimal128(default_decimal_precision, conversion_rate_scale)))
            elif scale in (-2, -3, -4):
                fields.append(pa.field(col, pa.decimal128(default_decimal_precision, 4)))
            else:
                fields.append(pa.field(col, pa.decimal128(default_decimal_precision, scale)))
        else:
            fields.append(pa.field(col, pa.string()))

    return pa.schema(fields)

def resolve_decimals_from_scale_flag(
        scale_flag: int | None,
        row: dict,
        currency_decimals: dict[str, int | None],
) -> int | None:
    
    if scale_flag is None:
        return None
    if scale_flag >= 0:
        return scale_flag
    
    if scale_flag == -2:
        code = row.get("currency_code_transaction_de_49")
    elif scale_flag == -3:
        code = row.get("currency_code_reconciliation_de_50")
    elif scale_flag == -4:
        code = row.get("currency_code_cardholder_billing_de_51")
    else:
        return None
    
    if code is None:
        return None
    
    code = str(code).zfill(3)
    return currency_decimals.get(code)
