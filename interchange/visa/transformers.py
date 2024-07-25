import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.files import FileStorage


log = Logger(__name__)
fs = FileStorage()


def load_as_ctf_format(client_id: str, file_id: str) -> pd.Series:
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


def transform_baseii_drafts(ctf_records: pd.Series) -> None:
    VALID_TC = ["05", "06", "07", "25", "26", "27"]
    VALID_TCSN = ["0", "1", "2", "3", "4", "5", "6", "7"]
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
    # FALTA GUARDAR EN NUEVO ARCHIVO COMO PARQUET!


def transform_baseii_vss(ctf_records: pd.Series) -> None:
    raise NotImplementedError


def transform_sms(ctf_records: pd.Series) -> None:
    raise NotImplementedError


ctf_data = load_as_ctf_format("DEMO", "2")
transform_baseii_drafts(ctf_data)
