from pathlib import Path
import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage
from interchange.mastercard.interchange.calculate_duck_db import (
    calculate_pre_eval, assign_rules, calculate_mastercard_fee
)


fs = FileStorage()
db = Database()
log = Logger(__name__)


def interchange_1240_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        txn_sub_dir: str = "400_IPM_1240_CLN",
        calc_sub_dir: str = "500_IPM_1240_CAL",
        target_sub_dir: str = "600_IPM_1240_ITX",
) -> None:

    log.logger.debug(f"[interchange_1240_fields] client={client_id} file={file_id}")
    #Buscar transaccionales
    txn_files = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=txn_sub_dir
    )

    #Buscar calculated
    calc_files = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=calc_sub_dir
    )

    log.logger.debug(f"Found {len(txn_files)} TXN files")
    log.logger.debug(f"Found {len(calc_files)} CAL files")

    #Crear índice por nombre base
    calc_by_key = {
        Path(p).stem: p
        for p in calc_files
    }

    # Iterar TXN
    for txn_path in txn_files:

        key = Path(txn_path).stem
        calc_path = calc_by_key.get(key)

        if not calc_path:
            log.logger.warning(f"No CAL match for {txn_path}")
            continue

        log.logger.debug(f"Processing pair:")
        log.logger.debug(f"  TXN: {txn_path}")
        log.logger.debug(f"  CAL: {calc_path}")
        try:
            df_evaluate = calculate_pre_eval(
                parquet_txn_path=txn_path,
                parquet_calc_path=calc_path,
                db=db,
                client_id=client_id,
                file_id=file_id
            )
            
            df_assign = assign_rules(df_eval=df_evaluate, db=db, extras=None, partition=None)

            df_fee = calculate_mastercard_fee(
                  df_assign=df_assign,
                  db=db,
                  brand_fx_eval="MASTERCARD",
            )
            
            out_fp = fs.build_target_parquet_filepath_from_raw(
                raw_filepath=txn_path,
                target_layer=target_layer,
                client_id=client_id,
                file_id=file_id,
                target_subdir=target_sub_dir,
                mti="1240"
             )

            log.logger.debug(f"write_parquet_by_filepath")
            fs.write_parquet_by_filepath(
                data=df_fee,
                filepath=out_fp,
                index=False,
                schema=None
            )

            log.logger.debug(f"Evaluate rows: {len(df_evaluate)}")

        except Exception as e:
            log.logger.error(f"Error processing {txn_path}: {str(e)}")
            continue

    log.logger.debug("Finished interchange_1240_fields")

def interchange_1442_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        txn_sub_dir: str = "400_IPM_1442_CLN",
        calc_sub_dir: str = "500_IPM_1442_CAL",
        target_sub_dir: str = "600_IPM_1442_ITX",
) -> None:

    log.logger.debug(f"[interchange_1442_fields] client={client_id} file={file_id}")
    #Buscar transaccionales
    txn_files = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=txn_sub_dir
    )

    #Buscar calculated
    calc_files = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=calc_sub_dir
    )

    log.logger.debug(f"Found {len(txn_files)} TXN files")
    log.logger.debug(f"Found {len(calc_files)} CAL files")

    #Crear índice por nombre base
    calc_by_key = {
        Path(p).stem: p
        for p in calc_files
    }

    # Iterar TXN
    for txn_path in txn_files:

        key = Path(txn_path).stem
        calc_path = calc_by_key.get(key)

        if not calc_path:
            log.logger.warning(f"No CAL match for {txn_path}")
            continue

        log.logger.debug(f"Processing pair:")
        log.logger.debug(f"  TXN: {txn_path}")
        log.logger.debug(f"  CAL: {calc_path}")
        try:
            df_evaluate = calculate_pre_eval(
                parquet_txn_path=txn_path,
                parquet_calc_path=calc_path,
                db=db,
                client_id=client_id,
                file_id=file_id
            )
            
            df_assign = assign_rules(df_eval=df_evaluate, db=db, extras=None, partition=None)

            df_fee = calculate_mastercard_fee(
                  df_assign=df_assign,
                  db=db,
                  brand_fx_eval="MASTERCARD",
            )
            
            out_fp = fs.build_target_parquet_filepath_from_raw(
                raw_filepath=txn_path,
                target_layer=target_layer,
                client_id=client_id,
                file_id=file_id,
                target_subdir=target_sub_dir,
                mti="1442"
             )

            log.logger.debug(f"write_parquet_by_filepath")
            fs.write_parquet_by_filepath(
                data=df_fee,
                filepath=out_fp,
                index=False,
                schema=None
            )

            log.logger.debug(f"Evaluate rows: {len(df_evaluate)}")

        except Exception as e:
            log.logger.error(f"Error processing {txn_path}: {str(e)}")
            continue

    log.logger.debug("Finished interchange_1442_fields")