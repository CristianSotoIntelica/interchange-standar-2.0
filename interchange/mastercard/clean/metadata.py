from interchange.persistence.database import Database
import pandas as pd

def base_clean_param() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"extract_name": "file_idn", "data_type": "string"},
            {"extract_name": "file_dt", "data_type": "string"},
            {"extract_name": "type_mti", "data_type": "string"},
            {"extract_name": "ref_id", "data_type": "int64"},
            {"extract_name": "function_code", "data_type": "int32"},
        ]
    )

def extend_field_defs_with_base_cols(field_defs: pd.DataFrame) -> pd.DataFrame:
    base_defs = base_clean_param()

    out = pd.concat([base_defs, field_defs], ignore_index=True)

    # Si hay duplicados (mismo extract_name), gana el primero (base_defs)
    out["extract_name"] = out["extract_name"].astype(str).str.strip()
    out = out.drop_duplicates(subset=["extract_name"], keep="first")

    return out

def load_mc_field_dtype_definitions() -> pd.DataFrame:
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