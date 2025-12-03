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


def store_vss_file(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    vss_types: list[str] = None,
    transactions_subdir_template: str = None,
    calculated_subdir_template: str = None,
    target_subdir_template: str = None,
) -> None:
    """
    Store processed VSS settlement files.
    Processes all VSS variants (110, 120, 130, 140) by default.
    
    Always merges clean transactions + calculated fields.
    
    Args:
        origin_layer: Source storage layer
        target_layer: Destination storage layer
        client_id: Client identifier
        file_id: File identifier
        vss_types: List of VSS types to process. Default: ["110", "120", "130", "140"]
        transactions_subdir_template: Template for clean data subdirs.
                                     Default: "300-BASEII_CLN_VSS_{vss_type}"
        calculated_subdir_template: Template for calculated subdirs.
                                   Default: "400-BASEII_CAL_VSS_{vss_type}"
        target_subdir_template: Template for output subdirs.
                               Default: "BASEII_VSS_{vss_type}"
    """
    if vss_types is None:
        vss_types = ["110", "120", "130", "140"]
    
    # Default templates (consistent with other modules)
    if transactions_subdir_template is None:
        transactions_subdir_template = "300-BASEII_CLN_VSS_{vss_type}"
    if calculated_subdir_template is None:
        calculated_subdir_template = "400-BASEII_CAL_VSS_{vss_type}"
    if target_subdir_template is None:
        target_subdir_template = "BASEII_VSS_{vss_type}"
    
    log.logger.info(f"Storing VSS variants: {', '.join(vss_types)}")
    
    for vss_type in vss_types:
        try:
            transactions_subdir = transactions_subdir_template.format(vss_type=vss_type)
            calculated_subdir = calculated_subdir_template.format(vss_type=vss_type)
            target_subdir = target_subdir_template.format(vss_type=vss_type)
            
            log.logger.info(f"Reading clean VSS {vss_type} records from {client_id} file {file_id}")
            transactions = fs.read_parquet(
                origin_layer,
                client_id,
                file_id,
                subdir=transactions_subdir,
            )
            
            log.logger.info(f"Reading calculated field data for VSS {vss_type} from {client_id} file {file_id}")
            calculated = fs.read_parquet(
                origin_layer,
                client_id,
                file_id,
                subdir=calculated_subdir,
            )
            
            log.logger.info(f"Merging VSS {vss_type} data from {client_id} file {file_id}")
            merged_data = transactions.join(calculated, how="left", lsuffix="_vss")
            
            log.logger.info(f"Saving VSS {vss_type} data for {client_id} file {file_id}")
            fs.write_parquet(
                merged_data, target_layer, client_id, file_id, subdir=target_subdir
            )
            
        except Exception as e:
            log.logger.error(f"Error storing VSS {vss_type}: {str(e)}")
            raise
