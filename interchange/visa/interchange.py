import numpy as np
import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage


log = Logger(__name__)
fs = FileStorage()


def calculate_baseii_interchange(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    transactions_subdir="300-BASEII_CLN_DRAFTS",
    calculated_subdir="400-BASEII_CAL_DRAFTS",
    target_subdir="500-BASEII_ITX_DRAFTS",
) -> None:
    """
    Calculate interchange fee fields for BASE II transaction data.
    """
    log.logger.info(
        f"Reading clean BASE II Transactions from {client_id} file {file_id}"
    )
    transactions = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=transactions_subdir,
    )
    log.logger.info(f"Reading calculated field data from {client_id} file {file_id}")
    calculated = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=calculated_subdir,
    )
    log.logger.info(
        f"Merging transactional and calculated data from {client_id} file {file_id}"
    )
    data = transactions.join(calculated, how="left", lsuffix="_baseii")
    pass


def calculate_sms_interchange() -> None:
    raise NotImplementedError


def calculate_vss_interchange() -> None:
    raise NotImplementedError


calculate_baseii_interchange(
    fs.Layer.STAGING, fs.Layer.STAGING, "DEMO", "CDA26F0BEB4349D03346A721DDCF0DC7"
)
