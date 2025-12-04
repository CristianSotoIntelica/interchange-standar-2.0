from datetime import datetime

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
            "column_name",
            "column_type",
            "float_decimals",
            "date_format",
        ],
        where={"type_record": type_record},
    )
    int_cols = ["float_decimals"]
    fd[int_cols] = fd[int_cols].apply(
        pd.to_numeric, downcast="integer", errors="coerce"
    )
    return fd.sort_values(sort_by, ascending=True)


def _retrieve_file_date(
    client_id: str,
    file_id: str,
) -> str:
    """
    Retrieve a file's processing date in 'YYYY-MM-DD' string format.
    """
    db = Database()
    file_date = db.read_records(
        table_name="file_control",
        fields=[
            "brand_id",
            "file_type",
            "file_processing_date",
            "landing_file_name",
        ],
        where={
            "client_id": client_id,
            "file_id": file_id,
        },
    ).iloc[0]
    return file_date["file_processing_date"]


def _parse_dates(date_series: pd.Series, date_format: str, file_date: str) -> pd.Series:
    """
    Parse a series of formatted string dates into datetime objects.
    """
    FILE_DATE_FORMAT = "%Y-%m-%d"
    reference_date = datetime.strptime(file_date, FILE_DATE_FORMAT)
    match date_format:
        case date_format if date_format.startswith("%"):
            result = pd.to_datetime(date_series, format=date_format, errors="coerce")
        case "!MMDD":
            TARGET_FORMAT = "%Y%m%d"
            pre = str(reference_date.year) + date_series
            pre = pd.to_datetime(pre, format=TARGET_FORMAT, errors="coerce")
            pre.loc[pre > reference_date] = pre.loc[
                pre > reference_date
            ] - pd.DateOffset(years=1)  # type: ignore
            result = pre
        case "!YDDD":
            TARGET_FORMAT = "%y%j"
            pre = str(reference_date.year)[2] + date_series
            pre = pd.to_datetime(pre, format=TARGET_FORMAT, errors="coerce")
            pre.loc[pre > reference_date] = pre.loc[
                pre > reference_date
            ] - pd.DateOffset(years=10)  # type: ignore
            result = pre
        case _:
            raise NotImplementedError
    return result


def _clean_field_values(
    field_series: pd.Series, field_defs: pd.DataFrame, file_date: str
) -> pd.Series:
    """
    Perform data cleaning on a series of values depending on its target data type.
    """
    name = field_series.name
    definition = field_defs[field_defs["column_name"] == name].iloc[0]
    match definition["column_type"]:
        case "str":
            result = field_series.str.strip().replace("", " ")
        case "int":
            pre = field_series.str.strip()
            result = pd.to_numeric(pre, errors="coerce").astype("Int64")
        case "float":
            mapping = {
                "}": "0",
                "{": "0",
                "A": "1",
                "B": "2",
                "C": "3",
                "D": "4",
                "E": "5",
                "F": "6",
                "G": "7",
                "H": "8",
                "I": "9",
                "J": "1",
                "K": "2",
                "L": "3",
                "M": "4",
                "N": "5",
                "O": "6",
                "P": "7",
                "Q": "8",
                "R": "9",
            }
            float_decimals = definition["float_decimals"]
            if not float_decimals > 0:
                raise ValueError
            field_series = field_series.replace(mapping, regex=True)
            pre = field_series.str.strip()
            result = pd.to_numeric(pre, errors="coerce") / (10**float_decimals)
        case "date":
            date_format = definition["date_format"]
            if not date_format:
                raise ValueError
            pre = field_series.str.strip()
            result = _parse_dates(pre, date_format, file_date)
        case _:
            raise NotImplementedError
    return result


def clean_baseii_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="200-BASEII_EXT_DRAFTS",
    target_subdir="300-BASEII_CLN_DRAFTS",
) -> None:
    """
    Clean BASE II field values from extracted transaction data.
    """
    log.logger.info("Loading Visa Draft field definitions")
    field_defs = _load_visa_field_definitions("draft", sort_by=[])
    log.logger.info(f"Retrieving file processing date for {client_id} file {file_id}")
    file_date = _retrieve_file_date(client_id, file_id)
    log.logger.info(
        f"Reading extracted BASE II Transactions from {client_id} file {file_id}"
    )
    data = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=origin_subdir,
    )
    log.logger.info(
        f"Cleaning extracted BASE II Transactions from {client_id} file {file_id}"
    )
    fields = []
    for _, field_series in data.items():
        clean_field = _clean_field_values(field_series, field_defs, file_date)
        fields.append(clean_field)
    clean_df = pd.concat(fields, axis=1)
    log.logger.info(f"Saving Visa Draft clean fields from {client_id} file {file_id}")
    fs.write_parquet(clean_df, target_layer, client_id, file_id, subdir=target_subdir)


def clean_sms_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="200-SMS_EXT_MESSAGES",
    target_subdir="300-SMS_CLN_MESSAGES",
) -> None:
    """
    Clean SMS field values from extracted transaction data.
    """
    log.logger.info("Loading Visa SMS Messages field definitions")
    field_defs = _load_visa_field_definitions("sms", sort_by=[])
    log.logger.info(f"Retrieving file processing date for {client_id} file {file_id}")
    file_date = _retrieve_file_date(client_id, file_id)
    log.logger.info(
        f"Reading extracted BASE II Transactions from {client_id} file {file_id}"
    )
    data = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=origin_subdir,
    )
    log.logger.info(
        f"Cleaning extracted BASE II Transactions from {client_id} file {file_id}"
    )
    fields = []
    for _, field_series in data.items():
        clean_field = _clean_field_values(field_series, field_defs, file_date)
        fields.append(clean_field)
    clean_df = pd.concat(fields, axis=1)
    log.logger.info(f"Saving Visa Draft clean fields from {client_id} file {file_id}")
    fs.write_parquet(clean_df, target_layer, client_id, file_id, subdir=target_subdir)


def clean_vss_fields() -> None:
    raise NotImplementedError
