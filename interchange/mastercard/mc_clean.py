from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage


import re
from pathlib import Path
from collections import defaultdict
from interchange.persistence.database import Database
from interchange.mastercard.storage.extract_fc_1644_filepath import extract_fc_from_filepath
import pyarrow.parquet as pq
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

def clean_1644_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_sub_dir: str = "200_IPM_1644_EXT",
    target_subdir: str = "200_IPM_1644_CLN",
) -> None:

    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir,
    )

    #field_defs = _load_mc_field_definitions()

    for filepath in list_filepaths:
        fc = extract_fc_from_filepath(filepath)

        if fc not in VALID_FC:
            continue

        df = fs.read_parquet_by_filepath(client_id=client_id, file_id=file_id, filepath=filepath)
        table = pq.read_table(filepath)
        print(table.schema)
        # out_fp = fs.build_target_parquet_filepath_from_raw(
        #     raw_filepath=filepath,        
        #     target_layer=target_layer,
        #     client_id=client_id,
        #     file_id=file_id,
        #     target_subdir=target_subdir,
        #     mti="1644",
        #     fc=fc,
        # )

        # fs.write_parquet_by_filepath(df, out_fp, index=False)
