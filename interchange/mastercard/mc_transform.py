import pandas as pd

from pathlib import Path
from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage
from interchange.mastercard.utils.transform_de_1240 import filter_df_columns_de


log = Logger(__name__)
fs = FileStorage()

def transform_ipm_1240(
        origin_layer: FileStorage.Layer, target_layer: FileStorage.Layer, 
        client_id: str, file_id: str, origin_sub_dir: str="100_IPM_1240_RAW", 
        target_subir: str="200_IPM_1240_TRA"
) -> None:
    
    # 1) Obtener lista de parquets derivados
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, client_id=client_id, file_id=file_id, subdir=origin_sub_dir)
    
    # 2) Iterar la lista para leer los parquets
    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath(
        client_id=client_id, file_id=file_id, filepath=filepath)

        log.logger.debug(
            f"Read parquet: {filepath} | rows = {len(df)}"
        )

        df_de_only = filter_df_columns_de(
            client_id=client_id, file_id=file_id, df=df
        )

        print(df_de_only.head())

        print(f"Columns after DE filter ({len(df_de_only.columns)}):")
        print(df_de_only.columns.tolist())

    
    
    
