import pandas as pd 
from interchange.persistence.database import Database

def load_mc_field_definitions(db:Database) -> pd.DataFrame:
    """
    Load Mastercard DE/PDS field metadata and build the `field_mc` matching key.

    The metadata table provides the canonical mapping between parsed ISO8583
    technical fields (TLV / TAG / SUBFIELD) and standardized extract column names.

    The synthetic key `field_mc` is built as:
    - "{tlv_field}_{tag}"            when subfield is null or zero
    - "{tlv_field}_{tag}_{subfield}" when subfield is present and non-zero

    Parameters
    ----------
    db : Database
        Database connection used to read metadata.

    Returns
    -------
    pandas.DataFrame
        Field metadata with an additional `field_mc` column.
    """
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
    """
    Build a rename mapping for pandas: technical column name -> extract name.

    Parameters
    ----------
    db : Database
        Database connection used to read metadata.

    Returns
    -------
    dict[str, str]
        Mapping `{field_mc: extract_name}` for `DataFrame.rename(columns=...)`.
    """
    fd = load_mc_field_definitions(db)
    fd = fd.drop_duplicates(subset=["field_mc"], keep="first")
    return fd.set_index("field_mc")["extract_name"].to_dict()