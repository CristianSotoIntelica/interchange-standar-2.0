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
    origin_subdir="400-BASEII_CAL_DRAFTS",
    target_subdir="500-BASEII_ITX_DRAFTS",
) -> None:
    """
    Calculate interchange fee fields for BASE II transaction data.
    """
    pass


def calculate_sms_interchange() -> None:
    raise NotImplementedError


def calculate_vss_interchange() -> None:
    raise NotImplementedError
