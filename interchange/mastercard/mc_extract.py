from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage
from interchange.mastercard.storage.extract_fc_1644_filepath import extract_fc_from_filepath
from interchange.mastercard.extract.layout_keys import build_expected_keys
from interchange.mastercard.extract.field_defs import build_rename_map
from interchange.mastercard.extract.schema_validate import missing_layout_keys_in_parquet
from interchange.mastercard.extract.schema_fill import fill_mising_from_db
from interchange.mastercard.extract.nomalize import normalize_df_columns
from interchange.mastercard.extract.reorder_cols import (
    build_ordered_extract_names_from_layout_keys, reorder_df_columns
)

from interchange.mastercard.layouts.layout_1240 import DICT_DE_LYT_1240, DICT_PDS_LYT_1240
from interchange.mastercard.layouts.layout_1442 import DICT_DE_LYT_1442, DICT_PDS_LYT_1442
from interchange.mastercard.layouts.layout_1740 import DICT_DE_LYT_1740, DICT_PDS_LYT_1740

from interchange.persistence.database import Database
import pandas as pd
from interchange.mastercard.layouts.layout_1644 import extract_df_1644_by_fc, normalize_columns_1644

log = Logger(__name__)
fs = FileStorage()

# Function Codes (FC) supported for MTI 1644 extraction.
VALID_FC = {"685", "688", "691"}

def _load_mc_field_definitions() -> pd.DataFrame:
    """
    Load Mastercard DE/PDS field metadata from the database and build matching keys.

    This helper reads the cannonical mapping between parsed ISO8593 technical fields
    /TLV / TAG / SUBFIELD) and their standarized extract column names.

    The returned Dataframe includes a synthetic key 'field_mc', build as:
        - "{tlv_field}_{tag}"               when subfield is null or zero
        - "{tlv_field}_{tag}_{subfield}" when subfield is present and non-zero

    This key is later used to rename parquet columns produced by the parsing stage
    into business-friendly extract names.

    Returns
    -------
    - Duplicate 'field_mc' values (if any) must be resolved by the caller (typically 
    by keeping the first occurence).
    - This function does not perfom validation; it only loads and prepares metadata.
    """
    db = Database()
    fd = db.read_records(
        table_name="de_pds_extract_names",
        fields=[
            "tlv_field",
            "tag",
            "subfield",
            "extract_name",
            "data_type",
        ],
    )

    # Build synthetic matching key: tlv_field + tag [+ subfield if applicable]
    fd["field_mc"] = (
        fd["tlv_field"].astype(str)
        + "_"
        + fd["tag"].astype(str)
        + fd["subfield"].apply(
            lambda x: f"_{int(x)}" if pd.notna(x) and int(x) != 0 else ""
        )
    )

    return fd

def extract_1644_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_sub_dir: str = "200_IPM_1644_TRA",
    target_sub_dir: str = "300_IPM_1644_EXT",
    ) -> None:
    """
    Extract and standarize MTI 1644 parquet files for supported Function Codes (FC).

    Steps
    -----
    1) List parquet files from 'origin_layer/origin_sub_dir'
    2) For each file: detec FC from filepath and skip unsupported FCs
    3) Read parquet into a Dataframe
    4) Ensure FC is present as a column ('FUNCTION_CODE')
    5) Align schema by FC (select expected DE/PDS columns, create missing as NA, and order columns)
    6) Rename technical columns to standarized extract names (DB-driven mapping)
    7) Normalize final column names (semanitc renames + lowercase)
    8) Write the extracted parquet into 'target_layer/target_subdir'

    Side effects
    ------------
    Writes parquet files in the target layer. Skips unssuported FC fils silently.

    Notes
    ------
    - The FC-specific schema contract is defined in 'layout_1644.output_columns_1644_for_fc'.
    - Missing expected columns are created with 'pd.NA' inside 'extract_df_1644_by_fc'.
    """
    # 1) List input parquet files 
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    # Preload DB-driven rename mapping: field_mc -> extract_name
    field_defs = _load_mc_field_definitions()
    field_defs = field_defs.drop_duplicates(subset=["field_mc"], keep="first")
    rename_map = field_defs.set_index("field_mc")["extract_name"].to_dict()

    for filepath in list_filepaths:
        # 2) Detect FC from filepath and filter
        fc = extract_fc_from_filepath(filepath)
        if fc not in VALID_FC:
            continue

        # 3) Read parquet
        df = fs.read_parquet_by_filepath(
            client_id=client_id, file_id=file_id, filepath=filepath
        )

        # 4) Ensure FC column exists (required by downstream logic)
        df["FUNCTION_CODE"] = fc

        # 5) Align schema by FC (select + fill missing with NA + reorder)
        df = extract_df_1644_by_fc(df, fc)

        # 6) Rename technical columns -> standadized extract names
        df = df.rename(columns=rename_map)
        
        # 7) Normalize final columns (semantic renames + lowercase)
        df = normalize_columns_1644(df)
        df = normalize_df_columns(df)

        # 8) Write parquet
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,        
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1644",
            fc=fc,
        )

        fs.write_parquet_by_filepath(df, out_fp, index=False)

def extract_1240_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "200_IPM_1240_TRA",
        target_sub_dir: str = "300_IPM_1240_EXT",
        ) -> None:
    """
    Extract and standarized MTI 1240 parquet files into the expected schema/layout.

    Steps
    ------
    1) List parquet files from 'origin_layer/origin_sub_dir'
    2) Build DB-driven rename mapping for technical -> extract column names
    3) Build expected layout keys from official 1240 DE + PDS layouts
    4) Build ordered extract column list from DB metadata (layout-aware ordering)
    5) For each file: read parquet into a Dataframe
    6) Rename columns using DB mapping
    7) Normalize column names (strip, lowercase, snake_case rules, etc.)
    8) Validate schema vs expected layout keys (detec missing columns)
    9) Fill missing layout columns (typically as NULL/NA) using DB metadata
    10) Reorder columns to match the expected layout order (plus 'first_cols' at the  begining)
    11) Write the extracted parquet into 'target_layer/target_sub_dir'

    Side effects
    ------------
    Write parquet files in the target layer.

    Notes
    -----
    This function assumes upstream parsing already created a parquet where 
    DE/PDS fields exists as columns (technical names).
    """
    log.logger.debug("Start Extract_1240_fields")

    # 1) List input parquet files
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, 
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    # 2) Build DB-driven rename mapping
    db = Database()
    rename_map = build_rename_map(db)

    # 3) Build expected keys from official layouts
    keys_1240 = build_expected_keys(DICT_DE_LYT_1240, DICT_PDS_LYT_1240)

    # 4) Build ordered column list from DB metadata
    ordered_layout_cols = build_ordered_extract_names_from_layout_keys(db, keys_1240)
    
    for filepath in list_filepaths:
        # 5) Read parquet
        df = fs.read_parquet_by_filepath(
            client_id=client_id, file_id=file_id, filepath=filepath
        )

        # 6) Rename technical columns -> extract names
        df = df.rename(columns=rename_map)

        # 7) Normalize column names
        df = normalize_df_columns(df)

        # 8) Validate missing layout keys
        missing = missing_layout_keys_in_parquet(df=df, expected_keys=keys_1240)

        # 9) Fill missing layout columns (NULL/NA)
        if missing:
            log.logger.warning(
                f"MTI: 1240 | missing layout fileds: {missing[:20]}" 
                f"{' ...' if len(missing) > 20 else ''}"
            )
            df = fill_mising_from_db(df, db, missing)

        # 10) Reorder columns
        df = reorder_df_columns(
            df, 
            ordered_layout_cols,
            first_cols=["file_idn", "file_dt", "type_mti", "ref_id", "function_code"
            ],
        )
        
        # 11) Write parquet
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1240",
        )

        fs.write_parquet_by_filepath(df, out_fp, index=False)

def extract_1442_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "200_IPM_1442_TRA",
        target_sub_dir: str = "300_IPM_1442_EXT",
        ) -> None:
    """
    Extract and standarized MTI 1442 parquet files into the expected schema/layout.

    Steps
    ------
    1) List parquet files from 'origin_layer/origin_sub_dir'
    2) Build DB-driven rename mapping for technical -> extract column names
    3) Build expected layout keys from official 1442 DE + PDS layouts
    4) Build ordered extract column list from DB metadata (layout-aware ordering)
    5) For each file: read parquet into a Dataframe
    6) Rename columns using DB mapping
    7) Normalize column names (strip, lowercase, snake_case rules, etc.)
    8) Validate schema vs expected layout keys (detec missing columns)
    9) Fill missing layout columns (typically as NULL/NA) using DB metadata
    10) Reorder columns to match the expected layout order (plus 'first_cols' at the  begining)
    11) Write the extracted parquet into 'target_layer/target_sub_dir'

    Side effects
    ------------
    Write parquet files in the target layer.

    Notes
    -----
    This function assumes upstream parsing already created a parquet where 
    DE/PDS fields exists as columns (technical names).
    """
    log.logger.debug("Start Extract_1442_fields")

    # 1) List input parquet files
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, 
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    # 2) Build DB-driven rename mapping
    db = Database()
    rename_map = build_rename_map(db)

    # 3) Build expected keys from official layouts
    keys_1442 = build_expected_keys(DICT_DE_LYT_1442, DICT_PDS_LYT_1442)

    # 4) Build ordered column list from DB metadata
    ordered_layout_cols = build_ordered_extract_names_from_layout_keys(db, keys_1442)
    
    for filepath in list_filepaths:
        # 5) Read parquet
        df = fs.read_parquet_by_filepath(
            client_id=client_id, file_id=file_id, filepath=filepath
        )

        # 6) Rename technical columns -> extract names
        df = df.rename(columns=rename_map)

        # 7) Normalize column names
        df = normalize_df_columns(df)

        # 8) Validate missing layout keys
        missing = missing_layout_keys_in_parquet(df=df, expected_keys=keys_1442)

        # 9) Fill missing layout columns (NULL/NA)
        if missing:
            log.logger.warning(
                f"MTI: 1442 | missing layout fileds: {missing[:20]}" 
                f"{' ...' if len(missing) > 20 else ''}"
            )
            df = fill_mising_from_db(df, db, missing)

        # 10) Reorder columns
        df = reorder_df_columns(
            df, 
            ordered_layout_cols,
            first_cols=["file_idn", "file_dt", "type_mti", "ref_id", "function_code"
            ],
        )
        
        # 11) Write parquet
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1442",
        )

        fs.write_parquet_by_filepath(df, out_fp, index=False)

def extract_1740_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "200_IPM_1740_TRA",
        target_sub_dir: str = "300_IPM_1740_EXT",
) -> None:
    
    log.logger.debug("Start Extract_1740_fields")

    # 1) List input parquet files
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, 
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    # 2) Build DB-driven rename mapping
    db = Database()
    rename_map = build_rename_map(db)

    # 3) Build expected keys from official layouts
    keys_1740 = build_expected_keys(DICT_DE_LYT_1740, DICT_PDS_LYT_1740)

    # 4) Build ordered column list from DB metadata
    ordered_layout_cols = build_ordered_extract_names_from_layout_keys(db, keys_1740)
    
    for filepath in list_filepaths:
        # 5) Read parquet
        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)

        # 6) Rename technical columns -> extract names
        df = df.rename(columns=rename_map)

        # 7) Normalize column names
        df = normalize_df_columns(df)

        # 8) Validate missing layout keys
        missing = missing_layout_keys_in_parquet(df=df, expected_keys=keys_1740)

        # 9) Fill missing layout columns (NULL/NA)
        if missing:
            log.logger.warning(f"MTI: 1740 | missing layout fileds: {missing[:20]}{' ...' if len(missing) > 20 else ''}")
            df = fill_mising_from_db(df, db, missing)

        # 10) Reorder columns
        df = reorder_df_columns(
            df, 
            ordered_layout_cols,
            first_cols=["file_idn", "file_dt", "type_mti", "ref_id", "function_code"
            ],
        )
        
        # 11) Write parquet
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1740",
        )

        fs.write_parquet_by_filepath(df, out_fp, index=False)
