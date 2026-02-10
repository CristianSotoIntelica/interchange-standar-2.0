import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage

from interchange.mastercard.calculate.iar import calculate_iar_unique
from interchange.mastercard.calculate.enricher_duckdb import (
    calculate_pre2_duckdb, calculate_ex_rate_duckdb, calculate_settlement_report_duckdb
)
from interchange.mastercard.calculate.exclude_flag import (
    build_lookup_691,df_exclude_flag
)

fs = FileStorage()

db = Database()

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
        subdir=origin_sub_dir
    )

    df_iar_unique = None
    iar_file_dt = None

    lookup_691 = build_lookup_691(
        origin_layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        origin_sub_dir="400_IPM_1644_CLN"
    )

    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath(
            client_id=client_id,
            file_id=file_id,
            filepath=filepath
        )

        df_excluded = df_exclude_flag(df,filepath,lookup_691)

        file_dt_raw = str(df_excluded["file_dt"].iloc[0]).strip()

        if df_iar_unique is None or file_dt_raw != iar_file_dt:
            df_iar_unique = calculate_iar_unique(file_dt=file_dt_raw, db=db)
            iar_file_dt = file_dt_raw
        
        df_pre_2 = calculate_pre2_duckdb(
            parquet_path=filepath,
            db=db,
            df_iar_unique=df_iar_unique,
            client_id=client_id,
            file_id=file_id
        )

        df_ex_rate = calculate_ex_rate_duckdb(
            parquet_path=filepath,
            db=db,
            client_id=client_id,
            file_id=file_id,
            brand="Mastercard",
        )

        df_amount = calculate_settlement_report_duckdb(
            df_ex_rate=df_ex_rate,
            df_pre2=df_pre_2,
            db=db
        )

        # TODO: Realizar el paso de jurisdiccion. Y realizar pruebas

        

        
        


        
    

def calculate_1442_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "400_IPM_1442_CLN",
        target_sub_dir: str = "500_IPM_1442_CAL",
) -> None:
    pass