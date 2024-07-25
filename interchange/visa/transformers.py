import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.files import FileStorage


log = Logger(__name__)
fs = FileStorage()


def _load_as_ctf_format(client_id: str, file_id: str) -> pd.Series:
    """
    Load a Visa interchange file into memory forcing the CTF line format.
    """
    records = fs.read_plaintext(fs.Layer.LANDING, client_id, file_id)
    header_record = str(records.iloc[0, 0])
    if len(header_record) == 168:
        return records["lines"]
    if len(header_record) == 170:
        return records["lines"].str.slice(stop=2) + records["lines"].str.slice(start=4)
    log.logger.error("The Visa interchange file has an unknown line length")
    return pd.Series([], name="lines")


def transform_baseii_drafts(client_id: str, file_id: str) -> None:
    """
    Pivot draft sequence numbers into individual rows of complete raw transactions.
    """
    VALID_TC = ["05", "06", "07", "25", "26", "27"]
    VALID_TCSN = ["0", "1", "2", "3", "4", "5", "6", "7"]
    log.logger.info(f"Opening {client_id} Visa interchange file {file_id} as CTF")
    ctf_records = _load_as_ctf_format(client_id, file_id)
    log.logger.info(f"Extracting Raw BASE II Drafts from {client_id} file {file_id}")
    raw_drafts = ctf_records[
        ctf_records.str.slice(stop=2).isin(VALID_TC)
        & ctf_records.str.slice(start=3, stop=4).isin(VALID_TCSN)
    ]
    raw_drafts_df = raw_drafts.to_frame(name="record")
    raw_drafts_df["tcsn"] = (
        raw_drafts_df["record"].str.slice(start=3, stop=4).astype(int)
    )
    raw_drafts_df["transaction"] = (
        raw_drafts_df["tcsn"] < raw_drafts_df["tcsn"].shift(1)
    ).cumsum()
    drafts = raw_drafts_df.pivot(
        index="transaction", columns="tcsn", values="record"
    ).reindex(columns=range(8))
    log.logger.info(f"Saving Raw {client_id} BASE II Drafts from file {file_id}")
    fs.write_parquet(
        drafts, fs.Layer.STAGING, client_id, file_id, subdir="100-BASEII_RAW_DRAFTS"
    )


def transform_sms_messages(ctf_records: pd.Series) -> None:
    raise NotImplementedError


def transform_baseii_vss(ctf_records: pd.Series) -> None:
    raise NotImplementedError
