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
    """
    Quantize a Decimal to a fixed number of fractional digits.

    Parameters
    ----------
    d : Decimal 
        Input decimal value.
    scale: int
        Number of decimal places to keep.

    Returns
    -------
    Decimal
        Quantized value using ROUND_HALF_UP.
    """
    q = Decimal(1).scaleb(-scale)
    return d.quantize(q, rounding=ROUND_HALF_UP)

def to_implied_decimal(x, scale: int) -> Optional[Decimal]:
    """
    Convert a digit-only amount to Decimal applying implied decimals.

    Examples
    --------
    scale=2: "1234" -> Decimal("12.34")
    scale=0: "1234" -> Decimal("1234")

    Parameters
    ----------
    x: Any
        Input value (string/number). Empty/NA is treated as null.
    scale: int
        Implied decimals scale.

    Returns
    -------
    Decimal | None
        Converted Decimal, or None if value is null/invalid.

    Notes
    -----
    If the input already contains a decimal point (e.g. "12.34"), it is returned as-is 
    to avoid double scaling.
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
    """
    Parse Mastercard "scale-prefixed" numeric representation into Decimal.

    The value is encoded as:
    - first digit = exponent (number of decimal places)
    - remaining digits = mantissa

    Example
    -------
    "212345" -> exponent=2, mantissa=12345 -> Decimal("123.45")

    Parameters
    ----------
    x : Any
        Raw value (string/number). Empty/NA returns None.
    out_scale : int | None, optional
        If provided, quantize output to this scale using ROUND_HALF_UP.

    Returns
    -------
    Decimal | None
        Parsed decimal value or None if invalid.
    """
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
    """
    Load currency decimals configuration from DB.

    Parameters
    ----------
    db : Database
        Database handle used to read the "currency" table.

    Returns
    -------
    dict[str, int | None]
        Mapping currency_numeric_code (zero-padded to 3) -> decimals (or None if missing).

    Side effects
    ------------
    Reads the "currency" table from the database.

    Raises
    ------
    TypeError
        If db.read_records() does not return a pandas DataFrame.
    """
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
    """
    Convert a digits-only amount to Decimal using per-row decimals.

    Parameters
    ----------
    amount_str : Any
        Amount string (digits, optional leading '-'). Empty/NA -> None.
    decimals : Any
        Decimals for this row (may be None/NA). If missing, default_decimals is used.
    default_decimals : int
        Fallback decimals when `decimals` cannot be resolved.
    out_scale : int
        Quantize output to this scale.

    Returns
    -------
    Decimal | None
        Converted and quantized Decimal, or None if invalid.

    Notes
    -----
    - Rejects non-digit amounts (after stripping sign).
    - Applies ROUND_HALF_UP quantization.
    """
    
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
    """
    Cast DataFrame columns based on metadata definitions.

    This function is metadata-driven:
    - `param` defines the expected column names (extract_name) and how to cast them (data_type).
    - Decimal behavior is controlled by `float_decimals` flags when present.

    Steps
    -----
    1) Normalize metadata (`param`) to extract (extract_name, data_type, float_decimals).
    2) For each metadata row:
       - If the column exists in `df`, cast according to `data_type`.
       - Decimal columns use float_decimals scale flags (see Notes).
    3) If dynamic decimal fields (-2/-3/-4) are present:
       - Resolve per-row decimals via currency codes and the currency table (or provided cache).
       - Convert and quantize to `dynamic_decimal_out_scale`.
    4) Reorder output columns deterministically:
       - First: columns that exist and are defined in metadata, in the same order as `param.extract_name`.
       - Then: any "extra" columns present in the input `df` but not defined in metadata,
         preserving the original input column order.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame (typically from extracted parquet).
    param : pd.DataFrame
        Metadata table with at least columns: extract_name, data_type.
        Optional: float_decimals.
    date_format : str, default "%Y%m%d"
        Format for 'date' columns (used with pd.to_datetime).
    timestamp_format : str | None, default None
        Format for 'timestamp' columns. If provided, values are normalized then parsed with this format.
    default_decimal_scale : int, default 2
        Default implied decimals scale for decimal fields when float_decimals is missing/NA.
    conversion_rate_scale : int, default 9
        Output scale for scale-prefixed decimals (float_decimals == -1).
    dynamic_decimal_out_scale : int, default 4
        Output scale for dynamic decimals (float_decimals in {-2,-3,-4}).
    currency_decimals_map : dict[str, int | None] | None, default None
        Optional cache mapping currency codes -> decimals. If None and dynamic decimals exist,
        the function loads it from DB.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with casted columns and stable ordering.

    Side effects
    ------------
    - Emits warnings through logger when values cannot be parsed (coerced to null).
    - May read the currency table from DB if dynamic decimals are required and
      `currency_decimals_map` is not provided.

    Notes
    -----
    Supported `data_type` values:
    - "int64": cast to pandas nullable Int64
    - "string": cast to pandas string dtype
    - "timestamp": datetime64 (format-driven if timestamp_format provided)
    - "date": python date objects (parsed with date_format)
    - "time": stored as "HH:MM:SS" string, padded to 6 digits
    - "decimal": Decimal with scale rules driven by float_decimals

    float_decimals flags for decimals:
    - >= 0: implied decimals scale (to_implied_decimal)
    - -1: scale-prefixed decimals (to_scale_prefixed_decimal), quantized to conversion_rate_scale
    - -2/-3/-4: dynamic decimals based on currency code columns:
        -2 -> currency_code_transaction_de_49
        -3 -> currency_code_reconciliation_de_50
        -4 -> currency_code_cardholder_billing_de_51

    Missing columns:
    - If a column defined in metadata does not exist in `df`, it is skipped (not created).
      If you need missing columns to exist as NULL, ensure they are created upstream (extract/fill step),
      or add a "create-missing-columns" step before reordering.
    """
    
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

    # Collect dynamic-decimal columns to process after we can resolve per-row currency decimals.
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

            # Decimal casting supports three modes:
            # 1) implied decimals (scale >= 0)
            # 2) scale-prefixed decimals (scale == -1)
            # 3) dynamic implied decimals (scale in {-2,-3,-4} based on currency code columns)
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

        meta_cols = p["extract_name"].tolist()
        
        ordered = [c for c in meta_cols if c in out.columns]
        ordered_set = set(ordered)

        extras = [c for c in out.columns if c not in ordered_set]

        out = out.loc[:, ordered + extras]

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
    """
    Build a PyArrow schema that matches metadata-driven casting rules.

    Steps
    -----
    1) Normalize metadata (`param`) to extract (extract_name, data_type, float_decimals).
    2) Map each (data_type, float_decimals) into a PyArrow field definition.
    3) Emit schema fields in the requested order:
       - If `ordered_cols` is provided: schema follows exactly that order (recommended when writing parquet).
       - Otherwise: schema follows `param.extract_name` order.

    Parameters
    ----------
    param : pd.DataFrame
        Metadata table with at least columns: extract_name, data_type.
        Optional: float_decimals.
    ordered_cols : Sequence[str] | None, optional
        Explicit final column order for the Arrow schema. This is typically set to the
        final dataframe column order (e.g., list(df_cast.columns)) to guarantee exact match.
        Columns present in `ordered_cols` but missing from metadata fall back to string type.
    ...
    """
    cols = ["extract_name", "data_type"]
    has_scale = "float_decimals" in param.columns
    if has_scale:
        cols.append("float_decimals")

    # Normalize metadata to ensure consistent matching and casting decisions.
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
