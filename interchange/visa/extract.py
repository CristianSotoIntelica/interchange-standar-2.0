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
    return fd


def extract_baseii_drafts(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="100-BASEII_RAW_DRAFTS",
    target_subdir="200-BASEII_EXT_DRAFTS",
) -> None:
    pass
