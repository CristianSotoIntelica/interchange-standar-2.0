import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage


log = Logger(__name__)
fs = FileStorage()


def _load_as_ctf(
    layer: FileStorage.Layer, client_id: str, file_id: str, subdir=""
) -> pd.Series:
    """
    Load a Visa interchange file into memory forcing the CTF line format.
    """
    records = fs.read_plaintext(fs.Layer.LANDING, client_id, file_id, subdir=subdir)
    header_record = str(records.iloc[0, 0])
    if len(header_record) == 168:
        return records["lines"]
    if len(header_record) == 170:
        return records["lines"].str.slice(stop=2) + records["lines"].str.slice(start=4)
    log.logger.error("The Visa interchange file has an unknown line length")
    return pd.Series([], name="lines")


def _pivot_values_on_key(values: pd.Series, start: int, stop: int, cols: list[str]):
    """
    Pivot a series of values into records by a sorted numerical key in values.
    """
    values_df = values.to_frame(name="value")
    values_df["key"] = values_df["value"].str.slice(start=start, stop=stop).astype(int)
    values_df["record"] = (values_df["key"] <= values_df["key"].shift(1)).cumsum()
    values_df["key"] = values_df["key"].astype(str)
    return (
        values_df.pivot(index="record", columns="key", values="value")
        .reindex(columns=cols)
        .fillna("")
        .astype(str)
    )


def transform_baseii_drafts(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="",
    target_subdir="100-BASEII_RAW_DRAFTS",
) -> None:
    """
    Reorganize drafts into individual records of raw transaction data.
    """
    VALID_TC = ["05", "06", "07", "25", "26", "27"]
    VALID_TCSN = ["0", "1", "2", "3", "4", "5", "6", "7"]
    log.logger.info(f"Opening {client_id} file {file_id} as CTF")
    ctf_records = _load_as_ctf(origin_layer, client_id, file_id, subdir=origin_subdir)
    log.logger.info(f"Extracting Raw BASE II Drafts from {client_id} file {file_id}")
    drafts = ctf_records[
        ctf_records.str.slice(stop=2).isin(VALID_TC)
        & ctf_records.str.slice(start=3, stop=4).isin(VALID_TCSN)
    ]
    drafts_df = _pivot_values_on_key(drafts, start=3, stop=4, cols=VALID_TCSN)
    log.logger.info(f"Saving Raw BASE II Transactions from {client_id} file {file_id}")
    fs.write_parquet(drafts_df, target_layer, client_id, file_id, subdir=target_subdir)


def transform_sms_messages(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="",
    target_subdir="100-SMS_RAW_MESSAGES",
) -> None:
    """
    Reorganize messages into individual records of raw transaction data.
    """
    VALID_TC = ["33"]
    VALID_TCSN = ["0"]
    VALID_SMS_TYPES = ["SMSRAWDATA"]
    VALID_RAW_DATA_VERSION = ["V22"]
    VALID_RECORD_TYPES = [
        # "22000",
        "22200",
        "22210",
        "22220",
        "22225",
        "22226",
        "22230",
        "22250",
        "22260",
        "22261",
        "22280",
        "22281",
        "22282",
    ]
    log.logger.info(f"Opening {client_id} file {file_id} as CTF")
    ctf_records = _load_as_ctf(origin_layer, client_id, file_id, subdir=origin_subdir)
    log.logger.info(f"Extracting Raw SMS Messages from {client_id} file {file_id}")
    drafts = ctf_records[
        ctf_records.str.slice(stop=2).isin(VALID_TC)
        & ctf_records.str.slice(start=3, stop=4).isin(VALID_TCSN)
        & ctf_records.str.slice(start=16, stop=26).isin(VALID_SMS_TYPES)
        & ctf_records.str.slice(start=34, stop=37).isin(VALID_RAW_DATA_VERSION)
    ]
    drafts_df = _pivot_values_on_key(drafts, start=35, stop=40, cols=VALID_RECORD_TYPES)
    log.logger.info(f"Saving Raw SMS Transactions from {client_id} file {file_id}")
    fs.write_parquet(drafts_df, target_layer, client_id, file_id, subdir=target_subdir)


def transform_vss_records() -> None:
    raise NotImplementedError
