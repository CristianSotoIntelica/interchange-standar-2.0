import pandas as pd

from pathlib import Path
from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage
from interchange.mastercard.transform.transform import (
    filter_df_columns_de,
    expand_subfields,
    reorder_with_subfield,
)

from interchange.mastercard.transform.pds_orchestrator import apply_pds_for_mti

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

        # log.logger.debug(
        #     f"Read parquet after DE filter:     {filepath} | rows = {len(df)} | cols: {len(df.columns)}\n"
        #     f"Cols Names: {df.columns.to_list()}"
        # )

        # 3) Filtrar las columnas segun el layout del mensaje
        # df_de_only = filter_df_columns_de(
        #     client_id=client_id, file_id=file_id, df=df
        # )

        df_de_only = filter_df_columns_de(df=df, mti = '1240')

        # log.logger.debug(
        #     f"Read parquet before DE filter:     {filepath} | rows = {len(df_de_only)} | cols: {len(df_de_only.columns)}\n"
        #     f"Cols Names: {df_de_only.columns.to_list()}"
        # )

        # 4) Expandir los DE por subfields según el layout del mensaje
        df_expand = expand_subfields(df=df_de_only, mti='1240')

        # log.logger.debug(
        #     f"Read parquet DE subfields:         {filepath} | rows = {len(df_expand)} | cols: {len(df_expand.columns)}\n"
        #     f"Cols Names: {df_expand.columns.to_list()}"
        # )        

        df_expand = reorder_with_subfield(df=df_expand, mti='1240')

        # log.logger.debug(
        #     f"Read parquet reorder DE:           {filepath} | rows = {len(df_expand)} | cols: {len(df_expand.columns)}\n"
        #     f"Cols Names: {df_expand.columns.to_list()}"
        # )

        # 5) Logica los PDS y los PDS subfields
        #df_expand = apply_pds_for_mti_1240(df=df_expand)
        df_expand = apply_pds_for_mti(df=df_expand, mti = '1240')
        # log.logger.debug(
        #     f"Read parquet PDS + subfields:      {filepath} | rows = {len(df_expand)} | cols: {len(df_expand.columns)}\n"
        #     f"Cols Names: {df_expand.columns.to_list()}"
        # )

        # 6) Generar parquets

        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath, target_layer=target_layer, client_id=client_id,
            file_id=file_id, target_subdir=target_subir
        )

        fs.write_parquet_by_filepath(df_expand, out_fp, index=False)

def transform_ipm_1644(
        origin_layer: FileStorage.Layer, target_layer: FileStorage.Layer, 
        client_id: str, file_id: str, origin_sub_dir: str="100_IPM_1644_RAW", 
        target_subir: str="200_IPM_1644_TRA"
) -> None:
    
    # 1) Obtener lista de parquets derivados
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, client_id=client_id, file_id=file_id, subdir=origin_sub_dir)
    
    # 2) Iterar la lista para leer los parquets
    for filepath in list_filepaths:
        
        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)

        # 3) Filtrar las columnas segun el layout del mensaje
        #df_de_only = filter_df_columns_de(client_id=client_id, file_id=file_id, df=df)

        df_de_only = filter_df_columns_de(df=df, mti = '1644')
        # 4) Expandir los DE por subfields según el layout del mensaje
        df_expand = expand_subfields(df=df_de_only, mti='1644')    

        df_expand = reorder_with_subfield(df=df_expand, mti ='1644')

        # 5) Logica los PDS y los PDS subfields
        df_expand = apply_pds_for_mti(df=df_expand, mti = '1644')

        # 6) Generar parquets

        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath, target_layer=target_layer, client_id=client_id,
            file_id=file_id, target_subdir=target_subir
        )

        fs.write_parquet_by_filepath(df_expand, out_fp, index=False)

def transform_ipm_1740(
        origin_layer: FileStorage.Layer, target_layer: FileStorage.Layer, 
        client_id: str, file_id: str, origin_sub_dir: str="100_IPM_1740_RAW", 
        target_subir: str="200_IPM_1740_TRA"
) -> None:
    
    # 1) Obtener lista de parquets derivados
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, client_id=client_id, file_id=file_id, subdir=origin_sub_dir)
    
    # 2) Iterar la lista para leer los parquets
    for filepath in list_filepaths:
        
        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)

        # 3) Filtrar las columnas segun el layout del mensaje
        #df_de_only = filter_df_columns_de(client_id=client_id, file_id=file_id, df=df)

        df_de_only = filter_df_columns_de(df=df, mti = '1740')
        # 4) Expandir los DE por subfields según el layout del mensaje
        df_expand = expand_subfields(df=df_de_only, mti='1740')    

        df_expand = reorder_with_subfield(df=df_expand, mti ='1740')

        # 5) Logica los PDS y los PDS subfields
        df_expand = apply_pds_for_mti(df=df_expand, mti = '1740')

        # 6) Generar parquets

        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath, target_layer=target_layer, client_id=client_id,
            file_id=file_id, target_subdir=target_subir
        )

        fs.write_parquet_by_filepath(df_expand, out_fp, index=False)

