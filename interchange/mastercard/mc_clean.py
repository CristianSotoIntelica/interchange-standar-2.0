from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage

from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from decimal import Decimal, InvalidOperation

from interchange.persistence.database import Database
from interchange.mastercard.storage.extract_fc_1644_filepath import extract_fc_from_filepath


log = Logger(__name__)
fs = FileStorage()

VALID_FC = {"685", "688", "691"}

def _load_mc_field_dtype_definitions() -> pd.DataFrame:
    """
    Esperado en BD:
      - extract_name: str
      - data_type: str  -> {'int64','string','decimal','timestamp','date'}
      - float_decimals: int -> scale para implied decimals (ej. 2 => /100)
    """
    db = Database()
    fd = db.read_records(
        table_name="de_pds_extract_names",
        fields=["extract_name", "data_type", "float_decimals"],
    )

    fd = fd.copy()
    fd["extract_name"] = fd["extract_name"].astype(str).str.strip()
    fd["data_type"] = fd["data_type"].astype(str).str.strip().str.lower()

    if "float_decimals" in fd.columns:
        fd["float_decimals"] = pd.to_numeric(fd["float_decimals"], errors="coerce").astype("Int64")

    return fd


# ============================================================
# Conversión decimal
# ============================================================
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


# ============================================================
# Normalización pandas
# ============================================================

def cast_df_from_param(
    df: pd.DataFrame,
    param: pd.DataFrame,
    *,
    date_format: str = "%Y%m%d",
    timestamp_format: Optional[str] = None,
    default_decimal_scale: int = 2,
) -> pd.DataFrame:
    """
    - Metadata-driven: usa param como fuente de verdad.
    - Limpia y castea en pandas (valores).
    - 'decimal' aplica implied decimals según float_decimals (scale por columna).
    - 'date' usa formato explícito para evitar warnings y parsing inconsistente.
    """
    out = df.copy()

    # Solo columnas necesarias + normalización
    cols = ["extract_name", "data_type"]
    has_scale = "float_decimals" in param.columns
    if has_scale:
        cols.append("float_decimals")

    p = param[cols].copy()
    p["extract_name"] = p["extract_name"].astype(str).str.strip()
    p["data_type"] = p["data_type"].astype(str).str.strip().str.lower()
    if has_scale:
        p["float_decimals"] = pd.to_numeric(p["float_decimals"], errors="coerce").astype("Int64")

    # ---- Casteo por columna según metadata ----
    for _, row in p.iterrows():
        col = row["extract_name"]
        t = row["data_type"]

        if col not in out.columns:
            continue

        if t == "int64":
            # nullable int
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

        elif t == "string":
            out[col] = out[col].astype("string")

        elif t == "timestamp":
            if timestamp_format:
                out[col] = pd.to_datetime(out[col], format=timestamp_format, errors="coerce")
            else:
                out[col] = pd.to_datetime(out[col], errors="coerce")

        elif t == "date":
            out[col] = pd.to_datetime(out[col], format=date_format, errors="coerce").dt.date

        elif t == "decimal":
            # scale por columna (implied decimals)
            scale = default_decimal_scale
            if has_scale and pd.notna(row["float_decimals"]):
                scale = int(row["float_decimals"])

            src = out[col]
            converted = src.apply(lambda x: to_implied_decimal(x, scale))

            # Log pro: detecta valores no nulos que terminaron NULL por parsing inválido
            bad = int(src.notna().sum() - pd.Series(converted).notna().sum())
            if bad > 0:
                log.warning(f"[cast_df_from_param] Columna '{col}' tuvo {bad} valores inválidos -> NULL")

            out[col] = converted

        else:
            out[col] = out[col].astype("string")

    return out

def build_arrow_schema_from_param(
    param: pd.DataFrame,
    *,
    default_decimal_precision: int = 18,
    default_decimal_scale: int = 2,
    timestamp_unit: str = "ns",
) -> pa.Schema:
    """
    Importante: el scale usado aquí debe coincidir con el scale aplicado en cast_df_from_param
    (float_decimals -> decimal(p, scale)).
    """
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

        elif t == "decimal":
            scale = default_decimal_scale
            if has_scale and pd.notna(row["float_decimals"]):
                scale = int(row["float_decimals"])
            fields.append(pa.field(col, pa.decimal128(default_decimal_precision, scale)))

        else:
            fields.append(pa.field(col, pa.string()))

    return pa.schema(fields)

    
def inspect_parquet_schema(path: str) -> None:
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(path)
    print("\n=== PARQUET SCHEMA ===")
    print(pf.schema_arrow)


def clean_1644_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_sub_dir: str = "200_IPM_1644_EXT",
    target_subdir: str = "200_IPM_1644_CLN",
) -> None:

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    field_defs = _load_mc_field_dtype_definitions()
    
    
    for filepath in list_filepaths:
        fc = extract_fc_from_filepath(filepath)

        if fc not in VALID_FC:
            continue

        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)
        df_cast = cast_df_from_param(df, field_defs)
       # df_cast.dtypes.head(15)
       
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,        
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_subdir,
            mti="1644",
            fc=fc,
        )

        schema = build_arrow_schema_from_param(
            field_defs,
            default_decimal_precision=18,
            default_decimal_scale=2,
            timestamp_unit="ns",
        )

        fs.write_parquet_by_filepath(df_cast, out_fp, index=False, schema=schema)
        #inspect_parquet_schema(out_fp)