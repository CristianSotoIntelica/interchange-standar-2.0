import pandas as pd 
import os

def classified_block_mti(df, test_path):
    sub_dfs = {}
 
    for (c1, c2), group in df.groupby(['block', 'mti']):
        key = f"name_{c1}_{c2}"
        sub_dfs[key] = group.reset_index(drop=True)
    for key, subdf in sub_dfs.items():
    
        col2_value = key.split('_')[-1]   # segundo parámetro real
        suffix = col2_value[-4:]          # RIGHT(4)
    
        if suffix == '1240':
            filepath = test_path / f"{key}.parquet" 
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)
        
        elif suffix == '1442':
            filepath = test_path / f"{key}.parquet"
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)
        
        elif suffix == '1644':
            filepath = test_path / f"{key}.parquet"
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)
        
        elif suffix == '1740':
            filepath = test_path / f"{key}.parquet"
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)
