from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage


log = Logger(__name__)
fs = FileStorage()


def store_baseii_file(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    transactions_subdir="300-BASEII_CLN_DRAFTS",
    calculated_subdir="400-BASEII_CAL_DRAFTS",
    interchange_subdir="500-BASEII_ITX_DRAFTS",
    target_subdir="BASEII_DRAFTS",
) -> None:
    """
    Store a fully processed BASE II file.
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
    log.logger.info(f"Reading interchange data from {client_id} file {file_id}")
    interchange = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=interchange_subdir,
    )
    log.logger.info(f"Merging full BASE II data from {client_id} file {file_id}")
    merged_data = transactions.join(calculated, how="left", lsuffix="_baseii")
    merged_data = merged_data.join(interchange, how="left", rsuffix="_intelica")
    log.logger.info(f"Saving full BASE II for {client_id} file {file_id}")
    fs.write_parquet(
        merged_data, target_layer, client_id, file_id, subdir=target_subdir
    )


def store_sms_file() -> None:
    raise NotImplementedError


def store_vss_file() -> None:
    raise NotImplementedError
