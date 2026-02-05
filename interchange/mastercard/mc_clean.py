from interchange.persistence.file import FileStorage

from interchange.mastercard.storage.extract_fc_1644_filepath import extract_fc_from_filepath
from interchange.mastercard.clean.fields_dtype_def import cast_df_from_params_def, build_arrow_schema_from_params
from interchange.mastercard.clean.metadata import load_mc_field_dtype_definitions, extend_field_defs_with_base_cols

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

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    field_defs = load_mc_field_dtype_definitions()
    field_defs = extend_field_defs_with_base_cols(field_defs)
    
    schema = build_arrow_schema_from_params(
            field_defs,
            default_decimal_precision=18,
            default_decimal_scale=2,
            timestamp_unit="ns",
    )
    
    for filepath in list_filepaths:
        fc = extract_fc_from_filepath(filepath)

        if fc not in VALID_FC_1644:
            continue

        df = fs.read_parquet_by_filepath(
            client_id=client_id, 
            file_id=file_id, 
            filepath=filepath
        )

        df_cast = cast_df_from_params_def(
            df=df, 
            param=field_defs
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
        
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir
    )

    fields_def = load_mc_field_dtype_definitions()

    schema = build_arrow_schema_from_params(
            param=fields_def,
            default_decimal_precision=18,
            default_decimal_scale=2,
            timestamp_unit="ns",
    )

    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath(
            client_id=client_id, 
            file_id=file_id, 
            filepath=filepath
        )
        
        df_cast = cast_df_from_params_def(
            df=df,
            param=fields_def,
            date_format="%y%m%d",
            timestamp_format="%y%m%d%H%M%S",
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
