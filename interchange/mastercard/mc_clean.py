from interchange.persistence.file import FileStorage

from interchange.mastercard.interpreter.storage.extract_fc_1644_filepath import extract_fc_from_filepath
from interchange.mastercard.clean.fields_dtype_def import (
    cast_df_from_params_def, 
    build_arrow_schema_from_params
)
from interchange.mastercard.clean.metadata import (
    load_mc_field_dtype_definitions, 
    extend_field_defs_with_base_cols
)

fs = FileStorage()

VALID_FC_1644 = {"685", "688", "691"}

def clean_1644_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_sub_dir: str = "300_IPM_1644_EXT",
    target_sub_dir: str = "400_IPM_1644_CLN",
) -> None:
    """
    Clean Mastercard MTI 1644 extracted parquet files.

    This step is metadata-driven: casting rules and output column ordering are derived from
    DB definitions (de_pds_extract_names) + a small set of required "base" columns.

    Steps
    -----
    1) List input parquet files from the EXT subdirectory.
    2) Load dtype rules from DB (extract_name, data_type, float_decimals).
    3) Extend metadata with required base columns (file_idn, file_dt, type_mti, ref_id, function_code).
    4) For each file:
        - Extract FC from the filepath and skip unsupported FCs.
        - Read parquet from EXT.
        - Cast/normalize columns according to metadata (types, implied decimals, date/time parsing).
        - Enforce a deterministic output column order (based on metadata + available columns).
        - Build a PyArrow schema aligned to the ordered output columns.
        - Write cleaned parquet to the CLN subdirectory using the Arrow schema.

    Parameters
    ----------
    origin_layer : FileStorage.Layer
        Source layer containing extracted parquet files.
    target_layer : FileStorage.Layer
        Target layer where cleaned parquet files are written.
    client_id : str
        Client identifier.
    file_id : str
        File batch identifier used by FileStorage path resolution.
    origin_sub_dir : str, default "300_IPM_1644_EXT"
        Subdirectory containing MTI 1644 extracted parquet files.
    target_sub_dir : str, default "400_IPM_1644_CLN"
        Subdirectory where MTI 1644 cleaned parquet files are written.
    
    Returns
    -------
    None

    Side effects
    ------------
    Reads and writes parquet files on disk. Queries DB metadata via
    load_mc_field_dtype_definitions(). Casting may emit warnings through logger.
    
    Notes
    -----
    - Only function codes in VALID_FC_1644 are processed; other files are skipped.
    - Column ordering is enforced before writing so downstream processes see a stable schema.
    """

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    # Metadata-driven casting rules (DB) + required base columns.
    field_defs = load_mc_field_dtype_definitions()
    field_defs = extend_field_defs_with_base_cols(field_defs)
    
    for filepath in list_filepaths:
        fc = extract_fc_from_filepath(filepath)

        if fc not in VALID_FC_1644:
            continue

        df = fs.read_parquet_by_filepath(
            client_id=client_id, 
            file_id=file_id, 
            filepath=filepath
        )

        # Cast + normalize. This step also ensures output columns follow a deterministic order.
        df_cast = cast_df_from_params_def(
            df=df, 
            param=field_defs
        )

        # Arrow schema must match the final column order written to parquet.
        schema = build_arrow_schema_from_params(
            field_defs,
            ordered_cols=list(df_cast.columns),
            default_decimal_precision=18,
            default_decimal_scale=2,
            timestamp_unit="ns",
         )

        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,        
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1644",
            fc=fc,
        )

        fs.write_parquet_by_filepath(
            df_cast, 
            out_fp, 
            index=False, 
            schema=schema
        )

def clean_1240_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "300_IPM_1240_EXT",
        target_sub_dir: str = "400_IPM_1240_CLN",
) -> None:
    """
    Clean Mastercard MTI 1240 extracted parquet files.

    This step is metadata-driven: casting rules and output column ordering are derived from
    DB definitions (de_pds_extract_names) + a small set of required "base" columns.

    Steps
    -----
    1) List input parquet files from the EXT subdirectory.
    2) Load dtype rules from DB (extract_name, data_type, float_decimals).
    3) Extend metadata with required base columns (file_idn, file_dt, type_mti, ref_id, function_code).
    4) For each file:
        - Read parquet from EXT.
        - Cast/normalize columns according to metadata (types, implied decimals, date/time parsing).
        - Enforce a deterministic output column order (based on metadata + available columns).
        - Build a PyArrow schema (once) aligned to the ordered output columns.
        - Write cleaned parquet to the CLN subdirectory using the Arrow schema.

    Parameters
    ----------
    origin_layer : FileStorage.Layer
        Source layer containing extracted parquet files.
    target_layer : FileStorage.Layer
        Target layer where cleaned parquet files are written.
    client_id : str
        Client identifier.
    file_id : str
        File batch identifier used by FileStorage path resolution.
    origin_sub_dir : str, default "300_IPM_1240_EXT"
        Subdirectory containing MTI 1240 extracted parquet files.
    target_sub_dir : str, default "400_IPM_1240_CLN"
        Subdirectory where MTI 1240 cleaned parquet files are written.

    Returns
    -------
    None

    Side effects
    ------------
    Reads and writes parquet files on disk. Queries DB metadata via
    load_mc_field_dtype_definitions(). Casting may emit warnings through logger.

    Notes
    -----
    - Uses 2-digit year formats: date_format="%y%m%d", timestamp_format="%y%m%d%H%M%S".
    - Schema is built from the first processed file and reused; assumes all files share compatible columns.
    """
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir
    )

    fields_defs = load_mc_field_dtype_definitions()
    fields_defs = extend_field_defs_with_base_cols(fields_defs)

    # Build the schema once (first file) to avoid repeating work.
    schema = None

    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath(
            client_id=client_id, 
            file_id=file_id, 
            filepath=filepath
        )
        
        df_cast = cast_df_from_params_def(
            df=df,
            param=fields_defs,
            date_format="%y%m%d",
            timestamp_format="%y%m%d%H%M%S",
        )

        if schema is None:
            # Arrow schema must match the final column order written to parquet.
            schema = build_arrow_schema_from_params(
                param=fields_defs,
                ordered_cols=list(df_cast.columns),
                default_decimal_precision=18,
                default_decimal_scale=2,
                timestamp_unit="ns",
            )


        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1240"
        )
            
        fs.write_parquet_by_filepath(
            df_cast, 
            out_fp, 
            index=False, 
            schema=schema
        )

def clean_1442_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,    
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "300_IPM_1442_EXT",
        target_sub_dir: str = "400_IPM_1442_CLN",
) -> None:
    """
    Clean Mastercard MTI 1442 extracted parquet files.

    This step is metadata-driven: casting rules and output column ordering are derived from
    DB definitions (de_pds_extract_names) + a small set of required "base" columns.

    Steps
    -----
    1) List input parquet files from the EXT subdirectory.
    2) Load dtype rules from DB (extract_name, data_type, float_decimals).
    3) Extend metadata with required base columns (file_idn, file_dt, type_mti, ref_id, function_code).
    4) For each file:
        - Read parquet from EXT.
        - Cast/normalize columns according to metadata (types, implied decimals, date/time parsing).
        - Enforce a deterministic output column order (based on metadata + available columns).
        - Build a PyArrow schema (once) aligned to the ordered output columns.
        - Write cleaned parquet to the CLN subdirectory using the Arrow schema.

    Parameters
    ----------
    origin_layer : FileStorage.Layer
        Source layer containing extracted parquet files.
    target_layer : FileStorage.Layer
        Target layer where cleaned parquet files are written.
    client_id : str
        Client identifier.
    file_id : str
        File batch identifier used by FileStorage path resolution.
    origin_sub_dir : str, default "300_IPM_1442_EXT"
        Subdirectory containing MTI 1442 extracted parquet files.
    target_sub_dir : str, default "400_IPM_1442_CLN"
        Subdirectory where MTI 1442 cleaned parquet files are written.

    Returns
    -------
    None

    Side effects
    ------------
    Reads and writes parquet files on disk. Queries DB metadata via
    load_mc_field_dtype_definitions(). Casting may emit warnings through logger.

    Notes
    -----
    - Uses 2-digit year formats: date_format="%y%m%d", timestamp_format="%y%m%d%H%M%S".
    - Schema is built from the first processed file and reused; assumes all files share compatible columns.
    """ 
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir
    )

    fields_defs = load_mc_field_dtype_definitions()
    fields_defs = extend_field_defs_with_base_cols(fields_defs)

    schema = None

    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath(
            client_id=client_id, 
            file_id=file_id, 
            filepath=filepath
        )
        
        df_cast = cast_df_from_params_def(
            df=df,
            param=fields_defs,
            date_format="%y%m%d",
            timestamp_format="%y%m%d%H%M%S",
        )

        if schema is None:
            # Arrow schema must match the final column order written to parquet.
            schema = build_arrow_schema_from_params(
                param=fields_defs,
                ordered_cols=list(df_cast.columns),
                default_decimal_precision=18,
                default_decimal_scale=2,
                timestamp_unit="ns",
            )

        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1442"
        )
            
        fs.write_parquet_by_filepath(
            df_cast, 
            out_fp, 
            index=False, 
            schema=schema
        )
