from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage
from interchange.mastercard.storage.extract_fc_1644_filepath import extract_fc_from_filepath
from interchange.mastercard.extract.layout_keys import build_expected_keys
from interchange.mastercard.extract.field_defs import build_rename_map
from interchange.mastercard.extract.schema_validate import missing_layout_keys_in_parquet
from interchange.mastercard.extract.schema_fill import fill_mising_from_db
from interchange.mastercard.extract.nomalize import normalize_df_columns
from interchange.mastercard.extract.reorder_cols import (
    build_ordered_extract_names_from_layout_keys, reorder_df_columns
)

from interchange.mastercard.layouts.layout_1240 import DICT_DE_LYT_1240, DICT_PDS_LYT_1240
from interchange.mastercard.layouts.layout_1740 import DICT_DE_LYT_1740, DICT_PDS_LYT_1740

import re
from pathlib import Path
from collections import defaultdict
from interchange.persistence.database import Database
import pandas as pd
from interchange.mastercard.layouts.layout_1644 import extract_df_1644_by_fc, normalize_columns_1644

log = Logger(__name__)
fs = FileStorage()

VALID_FC = {"685", "688", "691"}
#, sort_by: list[str]

def _load_mc_field_definitions() -> pd.DataFrame:
    db = Database()
    fd = db.read_records(
        table_name="de_pds_extract_names",
        fields=[
            "tlv_field",
            "tag",
            "subfield",
            "extract_name",
            "data_type",
        ],
    )

    # Llave para hacer match: tlv_field + tag + subfield (si subfield != 0)
    fd["field_mc"] = (
        fd["tlv_field"].astype(str)
        + "_"
        + fd["tag"].astype(str)
        + fd["subfield"].apply(
            lambda x: f"_{int(x)}" if pd.notna(x) and int(x) != 0 else ""
        )
    )

    return fd

def extract_1644_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_sub_dir: str = "200_IPM_1644_TRA",
    target_subdir: str = "200_IPM_1644_EXT",
) -> None:

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    field_defs = _load_mc_field_definitions()

    # Si hay duplicados de field_mc, nos quedamos con el primero (ya ordenado)
    field_defs = field_defs.drop_duplicates(subset=["field_mc"], keep="first")

    rename_map = field_defs.set_index("field_mc")["extract_name"].to_dict()

    for filepath in list_filepaths:
        fc = extract_fc_from_filepath(filepath)

        if fc not in VALID_FC:
            continue

        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)

    
        df["FUNCTION_CODE"] = fc

        if fc == '685':
            df = extract_df_1644_by_fc(df, "685")
        if fc  == '688':
            df = extract_df_1644_by_fc(df, "688")
        if fc == '691':
            df = extract_df_1644_by_fc(df, "691")

        df = df.rename(columns=rename_map)
        
        df = normalize_columns_1644(df)
        
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,        
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_subdir,
            mti="1644",
            fc=fc,
        )

        fs.write_parquet_by_filepath(df, out_fp, index=False)


def extract_1240_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "200_IPM_1240_TRA",
        target_sub_dir: str = "300_IPM_1240_EXT",
) -> None:
    
    log.logger.debug("Start Extract_1240_fields")

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, 
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    db = Database()
    rename_map = build_rename_map(db)

    # 1) expected keys from layouts
    keys_1240 = build_expected_keys(DICT_DE_LYT_1240, DICT_PDS_LYT_1240)

    ordered_layout_cols = build_ordered_extract_names_from_layout_keys(db, keys_1240)
    
    for filepath in list_filepaths:
        
        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)

        # 1) Rename headers from parquet
        df = df.rename(columns=rename_map)

        # 2) Normalice headers
        df = normalize_df_columns(df)

        # 3) Missings headers vs layout (list of missings headers)
        missing = missing_layout_keys_in_parquet(df=df, expected_keys=keys_1240)

        # 4) Completed headers missings 
        if missing:
            log.logger.warning(f"MTI: 1240 | missing layout fileds: {missing[:20]}{' ...' if len(missing) > 20 else ''}")
            df = fill_mising_from_db(df, db, missing)

        # 5) Reorder columns
        df = reorder_df_columns(
            df, 
            ordered_layout_cols,
            first_cols=["msg_no", "block", "mti", "enc", "function_code", "function_role", "parse_ok", "de_1"]
        )
        
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1240",
        )

        fs.write_parquet_by_filepath(df, out_fp, index=False)


def extract_1740_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "200_IPM_1740_TRA",
        target_sub_dir: str = "300_IPM_1740_EXT",
) -> None:
    
    log.logger.debug("Start Extract_1740_fields")

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, 
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    db = Database()
    rename_map = build_rename_map(db)

    # 1) expected keys from layouts
    keys_1740 = build_expected_keys(DICT_DE_LYT_1740, DICT_PDS_LYT_1740)

    ordered_layout_cols = build_ordered_extract_names_from_layout_keys(db, keys_1740)
    
    for filepath in list_filepaths:
        
        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)

        # 1) Rename headers from parquet
        df = df.rename(columns=rename_map)

        # 2) Normalice headers
        df = normalize_df_columns(df)

        # 3) Missings headers vs layout (list of missings headers)
        missing = missing_layout_keys_in_parquet(df=df, expected_keys=keys_1740)

        # 4) Completed headers missings 
        if missing:
            log.logger.warning(f"MTI: 1740 | missing layout fileds: {missing[:20]}{' ...' if len(missing) > 20 else ''}")
            df = fill_mising_from_db(df, db, missing)

        # 5) Reorder columns
        df = reorder_df_columns(
            df, 
            ordered_layout_cols,
            first_cols=["msg_no", "block", "mti", "enc", "function_code", "function_role", "parse_ok", "de_1"]
        )
        
        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,
            target_layer=target_layer,
            client_id=client_id,
            file_id=file_id,
            target_subdir=target_sub_dir,
            mti="1740",
        )

        fs.write_parquet_by_filepath(df, out_fp, index=False)
