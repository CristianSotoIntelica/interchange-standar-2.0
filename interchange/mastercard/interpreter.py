import io
import numpy as np
import pandas as pd
from pathlib import Path
from enum import Enum
from typing import BinaryIO, Optional

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage

from interchange.mastercard.io.unblock import unblock_1014
from interchange.mastercard.io.message_reader import read_len_prefixed_messages

from interchange.mastercard.iso8583.dataelements import Parameters
from interchange.mastercard.iso8583.parse_format import build_wide_row, extract_de24_fast

from interchange.mastercard.storage.classified_block_mti import (
    write_parquet_by_mti_block_streaming,
    _canonical_schema_from_de_spec,
    finalize_writers,
)

log = Logger(__name__)
fs = FileStorage()

DE_SPEC = Parameters().getdataelements()

def add_block_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    is_header = (df["function_code"].astype(str) == "697") & (df["mti"] == "1644")
    is_trailer = (df["function_code"].astype(str) == "695") & (df["mti"] == "1644")

    block = []
    current_block = 0
    open_block = False

    for h, t in zip(is_header, is_trailer):
        if h:
            current_block += 1
            open_block = True
            block.append(current_block)
        elif open_block:
            block.append(current_block)
        else:
            block.append(np.nan)

        if t:
            open_block = False  # ← cierre real del bloque

    df["block"] = block
    return df

def _load_as_binary(
        layer: FileStorage.Layer, client_id: str, file_id: str, subdir="") -> BinaryIO:
    return fs.read_binary(fs.Layer.LANDING, client_id, file_id, subdir, True)

def interpretate_msg(
        origin_layer, target_layer, client_id: str, file_id: str, origin_subdir="", 
        target_sub_dir="", test_path: str = "") -> None:
    
    # 1) Leer el archivo binario
    stream_file = _load_as_binary(
        origin_layer, client_id, file_id, subdir=origin_subdir)

    # 2) Elimina los bloqueantes
    db = Database()
    need_unblock = db.needs_unblock_for_file(client_id=client_id, file_id=file_id)

    if need_unblock:
        unblocked_bytes = unblock_1014(stream_file=stream_file)
    else:
        stream_file.seek(0)    
        unblocked_bytes = stream_file.read()

    # 3) Lee nuevamente al archivo binario nuevo, delvuele un arreglo de body/bitmap en HEX con su message type y lo guarda en un DF
    rows = read_len_prefixed_messages(io.BytesIO(unblocked_bytes))
    df = pd.DataFrame(rows)

    # 3) Obtiene el function code para generar los bloques, accede al data element 24
    mask_1644 = df["mti"].eq("1644") & df["parse_ok"].eq(True)

    df["function_code"] = None

    idx = df.index[mask_1644]

    df.loc[idx, "function_code"] = [
        extract_de24_fast(
            body_hex=df.at[i, "body_hex"], bitmap_hex=df.at[i, "bitmap_hex"],
            enc=df.at[i, "enc"], de_spec=DE_SPEC)
        for i in idx
    ]

    #4) Genera los bloques de acuerdo al function code y message type
    df = add_block_column(df)

    #5) Generar el dataframe final y obtiene los dataelements de acuerdo al bitmap y body
    BATCH_SIZE = 20000  

    records = df.to_dict("records")

    schema = _canonical_schema_from_de_spec(DE_SPEC)
    writers = {}  # key: (file_id, block, mti) -> ParquetWriter 

    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]

        df_wide_chunk = pd.DataFrame([
            build_wide_row(
                msg_no=int(r["msg_no"]), block=r.get("block"), mti=r.get("mti"),
                enc=r.get("enc"), function_code=r.get("function_code"),
                function_role=r.get("function_role"), parse_ok=r.get("parse_ok", False),
                bitmap_hex=r.get("bitmap_hex"), body_hex=r.get("body_hex"), 
                de_spec=DE_SPEC)
            for r in chunk
        ])

        # escribe / clasifica este bloque por el chunk obtenido
        write_parquet_by_mti_block_streaming(
                df_wide_chunk, fs=fs, target_layer=target_layer, client_id=client_id,
                file_id=file_id, schema=schema, writers=writers)

        # libera memoria explícitamente
        del df_wide_chunk

    finalize_writers(writers)


