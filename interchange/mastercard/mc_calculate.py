from interchange.persistence.file import FileStorage

from interchange.mastercard.storage.extract_fc_1644_filepath import extract_fc_from_filepath
from interchange.mastercard.clean.fields_dtype_def import cast_df_from_params_def, build_arrow_schema_from_params
from interchange.mastercard.clean.metadata import load_mc_field_dtype_definitions, extend_field_defs_with_base_cols

fs = FileStorage()

def add_flg_exclusion_transaction(


)

def calculate_1240_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_sub_dir: str = "400_IPM_1240_CLN",
    target_sub_dir: str = "500_IPM_1240_CAL",
) -> None:
 

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath_v2(
            client_id=client_id, 
            file_id=file_id, 
            filepath=filepath
        )