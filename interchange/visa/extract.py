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
    return fd.sort_values(sort_by, ascending=True)


def extract_baseii_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="100-BASEII_RAW_DRAFTS",
    target_subdir="200-BASEII_EXT_DRAFTS",
) -> None:
    """
    Extract specific BASE II fields from records of raw transaction data.
    """
    log.logger.info("Loading Visa Draft field definitions")
    field_defs = _load_visa_field_definitions(
        "draft", sort_by=["tcsn", "position", "secondary_identifier_len"]
    )
    log.logger.info(f"Reading Raw BASE II Transactions from {client_id} file {file_id}")
    data = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=origin_subdir,
    )
    log.logger.info(f"Extracting Visa Draft fields from {client_id} file {file_id}")
    fields = []
    for _, fd in field_defs.iterrows():
        if not fd["secondary_identifier"]:
            # Use entire dataframe.
            data_view = data
        else:
            # Filter for rows that match secondary condition.
            data_view = data[
                data[fd["tcsn"]].str.slice(
                    start=fd["secondary_identifier_pos"] - 1,
                    stop=fd["secondary_identifier_pos"]
                    + fd["secondary_identifier_len"]
                    - 1,
                )
                == fd["secondary_identifier"]
            ]
        # Get field values from data view.
        field = pd.Series(
            data_view[fd["tcsn"]].str.slice(
                start=fd["position"] - 1, stop=fd["position"] + fd["length"] - 1
            ),
            name=fd["column_name"],
        )
        fields.append(field)
    extract_df = pd.concat(fields, axis=1).fillna("").astype(str)
    log.logger.info(f"Saving Visa Draft fields from {client_id} file {file_id}")
    fs.write_parquet(extract_df, target_layer, client_id, file_id, subdir=target_subdir)


def extract_sms_fields() -> None:
    raise NotImplementedError


def extract_vss_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    vss_types: list[str] = None,
    origin_subdir_template: str = None,
    target_subdir_template: str = None,
) -> None:
    """
    Extract specific VSS fields from records of raw settlement data.
    Processes all VSS variants (110, 120, 130, 140) by default.
    
    Args:
        origin_layer: Source storage layer
        target_layer: Destination storage layer
        client_id: Client identifier
        file_id: File identifier
        vss_types: List of VSS types to process. Default: ["110", "120", "130", "140"]
        origin_subdir_template: Template for input subdirs (e.g., "100-BASEII_RAW_VSS_{vss_type}").
                               If None, uses default
        target_subdir_template: Template for output subdirs (e.g., "200-BASEII_EXT_VSS_{vss_type}").
                               If None, uses default
    """
    if vss_types is None:
        vss_types = ["110", "120", "130", "140"]
    
    # Default templates if not provided
    if origin_subdir_template is None:
        origin_subdir_template = "100-BASEII_RAW_VSS_{vss_type}"
    if target_subdir_template is None:
        target_subdir_template = "200-BASEII_EXT_VSS_{vss_type}"
    
    log.logger.info(f"Extracting fields for VSS variants: {', '.join(vss_types)}")
    
    for vss_type in vss_types:
        try:
            type_record = f"vss_{vss_type}"
            origin_subdir = origin_subdir_template.format(vss_type=vss_type)
            target_subdir = target_subdir_template.format(vss_type=vss_type)
            
            log.logger.info(f"Loading Visa {type_record} field definitions")
            field_defs = _load_visa_field_definitions(
                type_record, sort_by=["tcsn", "position", "secondary_identifier_len"]
            )
            log.logger.info(f"Reading Raw VSS {vss_type} records from {client_id} file {file_id}")
            data = fs.read_parquet(
                origin_layer,
                client_id,
                file_id,
                subdir=origin_subdir,
            )
            log.logger.info(f"Extracting Visa VSS {vss_type} fields from {client_id} file {file_id}")
            fields = []
            for _, fd in field_defs.iterrows():
                if not fd["secondary_identifier"]:
                    # Use entire dataframe.
                    data_view = data
                else:
                    # Filter for rows that match secondary condition.
                    data_view = data[
                        data[fd["tcsn"]].str.slice(
                            start=fd["secondary_identifier_pos"] - 1,
                            stop=fd["secondary_identifier_pos"]
                            + fd["secondary_identifier_len"]
                            - 1,
                        )
                        == fd["secondary_identifier"]
                    ]
                # Get field values from data view.
                field = pd.Series(
                    data_view[fd["tcsn"]].str.slice(
                        start=fd["position"] - 1, stop=fd["position"] + fd["length"] - 1
                    ),
                    name=fd["column_name"],
                )
                fields.append(field)
            extract_df = pd.concat(fields, axis=1).fillna("").astype(str)
            log.logger.info(f"Saving Visa VSS {vss_type} fields from {client_id} file {file_id}")
            fs.write_parquet(extract_df, target_layer, client_id, file_id, subdir=target_subdir)
            
        except Exception as e:
            log.logger.error(f"Error extracting VSS {vss_type}: {str(e)}")
            raise