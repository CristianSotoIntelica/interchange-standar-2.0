import pandas as pd 
from interchange.persistence.database import Database

def load_mc_field_definitions(db:Database) -> pd.DataFrame:
    fd = db.read_records(
        table_name="de_pds_extract_names",
        fields=["tlv_field", "tag", "subfield", "extract_name", "data_type"],
    )

    fd["field_mc"] = (
        fd["tlv_field"].astype("str") + 
        "_" + 
        fd["tag"].astype("str") + 
        fd["subfield"].apply(lambda x: f"_{int(x)}" if pd.notna(x) and int(x) != 0 else "")
    )
    return fd

def build_rename_map(db:Database) -> dict[str, str]:
    fd = load_mc_field_definitions(db)
    fd = fd.drop_duplicates(subset=["field_mc"], keep="first")
    return fd.set_index("field_mc")["extract_name"].to_dict()