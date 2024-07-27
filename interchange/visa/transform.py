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
    values_df["record"] = (values_df["key"] < values_df["key"].shift(1)).cumsum()
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
    log.logger.info(f"Saving Raw BASE II Drafts from {client_id} file {file_id}")
    fs.write_parquet(drafts_df, target_layer, client_id, file_id, subdir=target_subdir)


def transform_sms_messages() -> None:
    raise NotImplementedError


def transform_baseii_vss() -> None:
    raise NotImplementedError
