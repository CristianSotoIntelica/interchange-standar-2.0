from interchange.persistence.database import Database
import pandas as pd

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