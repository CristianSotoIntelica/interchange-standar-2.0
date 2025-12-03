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


def transform_sms_messages() -> None:
    raise NotImplementedError


def transform_vss_records(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="",
    vss_types: list[str] = None,
    target_subdir_template: str = None,   
) -> None:
    """
   Reorganize VSS settlement records into individual records of raw data.
    Processes all VSS variants (110, 120, 130, 140) by default.
    
    This function is optimized for processing multiple VSS types:
    - Loads file once
    - Pivots once
    - Applies specific filters for each VSS type
    
    Args:
        origin_layer: Source storage layer
        target_layer: Destination storage layer
        client_id: Client identifier
        file_id: File identifier
        origin_subdir: Optional subdirectory in origin layer
        vss_types: List of VSS types to process. Default: ["110", "120", "130", "140"]
        target_subdir_template: Template for output subdirs (e.g., "100-BASEII_RAW_VSS_{vss_type}").
                               If None, uses default: "100-BASEII_RAW_VSS_{vss_type}"

    """
    VSS_POS_START = 60
    VSS_POS_END = 63
    VSS_SUFFIX_START = 63
    VSS_SUFFIX_END = 65
    VSS_SUFFIX_VALUE = "  "
    VALID_VSS_TYPES = ["110", "120", "130", "140"]

    if vss_types is None:
        vss_types = VALID_VSS_TYPES.copy()
    
    # Default template if not provided
    if target_subdir_template is None:
        target_subdir_template = "100-BASEII_RAW_VSS_{vss_type}"

    VALID_TC = ["46"]
    VALID_TCSN = ["0","1"]

    log.logger.info(f"Opening {client_id} file {file_id} as CTF")
    ctf_records = _load_as_ctf(origin_layer, client_id, file_id, subdir=origin_subdir)
    log.logger.info(f"Extracting Raw VSS records (all types) from {client_id} file {file_id}")
    vss_records = ctf_records[
        ctf_records.str.slice(stop=2).isin(VALID_TC)
        & ctf_records.str.slice(start=3, stop=4).isin(VALID_TCSN)
    ]
    
    # Pivot once for all VSS records
    vss_df = _pivot_values_on_key(vss_records, start=3, stop=4, cols=VALID_TCSN)
    
    # Now filter and save for each VSS type
    log.logger.info(f"Processing VSS variants: {', '.join(vss_types)}")

    for vss_type in vss_types:
        try:
            if vss_type not in VALID_VSS_TYPES:
                log.logger.warning(f"Skipping invalid VSS type: {vss_type}")
                continue
            
            log.logger.info(f"Filtering for VSS type {vss_type}")
            vss_df_filtered = vss_df[
                (vss_df["0"].str.slice(start=VSS_POS_START, stop=VSS_POS_END) == vss_type)
                & (vss_df["0"].str.slice(start=VSS_SUFFIX_START, stop=VSS_SUFFIX_END) == VSS_SUFFIX_VALUE)
            ]

            if len(vss_df_filtered) == 0:
                log.logger.warning(f"No records found for VSS type {vss_type} in {client_id} file {file_id}")
                continue
                
            # Construct target subdir for this VSS type
            target_subdir = target_subdir_template.format(vss_type=vss_type)
            log.logger.info(f"Saving {len(vss_df_filtered)} Raw VSS {vss_type} records from {client_id} file {file_id}")
            fs.write_parquet(
                vss_df_filtered,
                target_layer,
                client_id,
                file_id,
                subdir=target_subdir,
            )

        except Exception as e:
            log.logger.error(f"Error processing VSS type {vss_type} from {client_id} file {file_id}: {e}")
            raise

    # Filter for specific VSS type
    # vss_df_filter = vss_df[
    #     (vss_df["0"].str.slice(start=60, stop=63) == vss_type)
    #     & (vss_df["0"].str.slice(start=63, stop=65) == "  ")
    # ]
    
    # log.logger.info(f"Saving Raw VSS {vss_type} records from {client_id} file {file_id}")
    # fs.write_parquet(vss_df_filter, target_layer, client_id, file_id, subdir=target_subdir)

