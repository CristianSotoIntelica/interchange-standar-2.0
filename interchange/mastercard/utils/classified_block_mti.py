import pandas as pd 
import os

from interchange.persistence.file import FileStorage
fs = FileStorage()

def classified_block_mti(
        df_data: pd.DataFrame, target_layer: FileStorage.Layer, client_id: str, 
        file_id: str):
    sub_dfs = {}
 
    for (c1, c2), group in df_data.groupby(['block', 'mti']):
        key = f"{c1}_{c2}"
        sub_dfs[key] = group.reset_index(drop=True)

    for key, subdf in sub_dfs.items():
    
        col2_value = key.split('_')[-1]   # segundo parámetro real
        suffix = col2_value[-4:]          # RIGHT(4)
    
        if suffix == '1240':
            fs.write_parquet_per_block(
                data=subdf, layer=target_layer, client_id=client_id, file_id=file_id, 
                subdir= "100_IPM_1240_RAW", name_block=key)
        
        elif suffix == '1442':
            fs.write_parquet_per_block(
                data=subdf, layer=target_layer, client_id=client_id, file_id=file_id, 
                subdir= "100_IPM_1442_RAW", name_block=key)
        
        elif suffix == '1644':
            fs.write_parquet_per_block(
                data=subdf, layer=target_layer, client_id=client_id, file_id=file_id, 
                subdir= "100_IPM_1644_RAW", name_block=key)
        
        elif suffix == '1740':
            fs.write_parquet_per_block(
                data=subdf, layer=target_layer, client_id=client_id, file_id=file_id, 
                subdir= "100_IPM_1740_RAW", name_block=key)
