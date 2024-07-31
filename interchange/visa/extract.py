import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage


log = Logger(__name__)
fs = FileStorage()


def _load_visa_field_definitions(type_record: str, sort_by: list[str]) -> pd.DataFrame:
    """
    Return a dataframe of Visa field definitions ordered by specific fields.
    """
    db = Database()
    fd = db.read_records(
        table_name="visa_fields",
        fields=[
            "type_record",
            "tcsn",
            "position",
            "length",
            "column_name",
            "secondary_identifier_pos",
            "secondary_identifier_len",
            "secondary_identifier",
        ],
        where={"type_record": type_record},
    )
    int_cols = [
        "position",
        "length",
        "secondary_identifier_pos",
        "secondary_identifier_len",
    ]
    fd[int_cols] = fd[int_cols].apply(
        pd.to_numeric, downcast="integer", errors="coerce"
    )
    return fd.sort_values(sort_by, ascending=True)


def extract_baseii_drafts(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="100-BASEII_RAW_DRAFTS",
    target_subdir="200-BASEII_EXT_DRAFTS",
) -> None:
    """
    Extract specific BASE II fields from records of raw transaction data.
    """
    log.logger.info("Loading Visa Draft field definitions")
    field_defs = _load_visa_field_definitions(
        "draft", sort_by=["tcsn", "position", "secondary_identifier_len"]
    )
    log.logger.info(f"Reading Raw BASE Transactions from {client_id} file {file_id}")
    data = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=origin_subdir,
    )
    log.logger.info(f"Extracting Visa Draft fields from {client_id} file {file_id}")
    fields = []
    for _, fd in field_defs.iterrows():
        if not fd["secondary_identifier"]:
            # Use entire dataframe.
            data_view = data
        else:
            # Filter for rows that match secondary condition.
            data_view = data[
                data[fd["tcsn"]].str.slice(
                    start=fd["secondary_identifier_pos"] - 1,
                    stop=fd["secondary_identifier_pos"]
                    + fd["secondary_identifier_len"]
                    - 1,
                )
                == fd["secondary_identifier"]
            ]
        # Get field values from data view.
        field = pd.Series(
            data_view[fd["tcsn"]].str.slice(
                start=fd["position"] - 1, stop=fd["position"] + fd["length"] - 1
            ),
            name=fd["column_name"],
        )
        fields.append(field)
    extract_df = pd.concat(fields, axis=1).fillna("").astype(str)
    log.logger.info(f"Saving Visa Draft fields from {client_id} file {file_id}")
    fs.write_parquet(extract_df, target_layer, client_id, file_id, subdir=target_subdir)


def extract_sms_messages() -> None:
    raise NotImplementedError


def extract_baseii_vss() -> None:
    raise NotImplementedError
