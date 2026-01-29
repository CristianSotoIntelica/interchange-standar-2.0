from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage


import re
from pathlib import Path
from collections import defaultdict
from interchange.persistence.database import Database
import pandas as pd

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

def extract_fc_from_filepath(filepath: str | Path) -> str:
    name = Path(filepath).name
    return name.rsplit("_", 1)[-1].replace(".parquet", "")

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
        df = df.rename(columns=rename_map)
        df["FUNCTION_CODE"] = fc

        out_fp = fs.build_target_parquet_filepath_from_raw(
            raw_filepath=filepath,        # <- EL MISMO ARCHIVO
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
        origin_sub_dir: str = "200_IPM_1240_EXT",
        target_sub_dir: str = "300_IPM_1240_TRA",
) -> None:
    
    log.logger.debug("Start Extract_1240_fields")
    
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer, 
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )
    

    field_defs = _load_mc_field_definitions()

    field_defs = field_defs.drop_duplicates(subset=["field_mc"], keep="first")

    rename_map = field_defs.set_index("field_mc")["extract_name"].to_dict()

    for filepath in list_filepaths:
        
        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)
        df = df.rename(columns=rename_map)
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"\s", "_", regex=True)
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
