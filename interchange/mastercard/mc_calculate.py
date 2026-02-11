import pandas as pd
from  pathlib import Path

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage

from interchange.mastercard.calculate.iar import calculate_iar_unique
from interchange.mastercard.calculate.enricher_duckdb import (
    calculate_pre2_duckdb, calculate_ex_rate_duckdb, calculate_settlement_report_duckdb
)

fs = FileStorage()

db = Database()

log = Logger(__name__)

def calculate_1240_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "400_IPM_1240_CLN",
        target_sub_dir: str = "500_IPM_1240_CAL",
) -> None:
    log.logger.debug(f"Searching for {client_id} file {file_id}")

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir
    )

    log.logger.debug(f"END Searching for {client_id} file {file_id}")

    df_iar_unique = None
    iar_file_dt = None

    out_dir = Path(r"C:\Users\daniel.olivera\Documents\Intelica\apps\interchange-standar-2.0\tst")

    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath(
            client_id=client_id,
            file_id=file_id,
            filepath=filepath
        )

        log.logger.debug(f"Reading parquet for {client_id} file {file_id}")

        file_dt_raw = str(df["file_dt"].iloc[0]).strip()

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

        df_iar_unique.head(1000).to_csv(out_dir / f"df_iar_unique.csv", index=False)
        df_pre_2.head(1000).to_csv(out_dir / f"df_pre_2.csv", index=False)
        df_ex_rate.head(1000).to_csv(out_dir / f"df_ex_rate.csv", index=False)
        df_amount.head(1000).to_csv(out_dir / f"df_amount.csv", index=False)

        log.logger.debug(f"END Reading parquet for {client_id} file {file_id}")
        break

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