import pandas as pd
from  pathlib import Path

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage

from interchange.mastercard.calculate.iar import calculate_iar_unique
from interchange.mastercard.calculate.enricher_duckdb import (
    calculate_pre2_duckdb, calculate_ex_rate_duckdb, calculate_settlement_report_duckdb
)
from interchange.mastercard.calculate.exclude_flag import (
    build_lookup_691, df_exclude_flag, 
)

from interchange.mastercard.calculate.calculate_fields import (
    build_mc_calculated_df, build_arrow_schema_from_layout
)

from interchange.mastercard.calculate.layout_calculate_fields import (
    CALCULATE_FIELDS_FINAL
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

    schema = None

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

        log.logger.debug(f"Reading parquet for {client_id} file {file_id}")

        file_dt_raw = str(df["file_dt"].iloc[0]).strip()

        df_excluded = df_exclude_flag(df,filepath,lookup_691)
        df_excluded.head(1000).to_csv(out_dir / f"df_excluded_flag.csv", index=False)

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

        df_final = build_mc_calculated_df(
            df_pre2=df_pre_2, 
            df_ex_rate=df_ex_rate,
            df_amount=df_amount, 
            dedupe_strategy="error",
        )

        if schema is None:
            schema = build_arrow_schema_from_layout(
                layout=CALCULATE_FIELDS_FINAL,
                ordered_cols=list(df_final.columns),
                default_decimal_precision=18,
                default_decimal_scale=4,
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
            df_final, 
            out_fp, 
            index=False, 
            schema=schema
        )


        df_iar_unique.head(1000).to_csv(out_dir / f"df_iar_unique.csv", index=False)
        df_pre_2.head(1000).to_csv(out_dir / f"df_pre_2.csv", index=False)
        df_ex_rate.head(1000).to_csv(out_dir / f"df_ex_rate.csv", index=False)
        df_amount.head(1000).to_csv(out_dir / f"df_amount.csv", index=False)
        df_final.head(10000).to_csv(out_dir / f"df_final.csv", index=False)

        log.logger.debug(f"END Reading parquet for {client_id} file {file_id}")
        break

        

        
        


        
    

def calculate_1442_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "400_IPM_1442_CLN",
        target_sub_dir: str = "500_IPM_1442_CAL",
) -> None:
    pass