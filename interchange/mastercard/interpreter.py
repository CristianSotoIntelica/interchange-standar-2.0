import io
from typing import BinaryIO, Optional, cast

import numpy as np
import pandas as pd

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
            block.append(float(current_block))
        elif open_block:
            block.append(float(current_block))
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
    rows = read_len_prefixed_messages(io.BytesIO(unblocked_bytes), as_hex=False)
    df = pd.DataFrame(rows)

    del rows
    
    # asegurar bool real (evita objetos raros)
    if "parse_ok" in df.columns:
        df["parse_ok"] = df["parse_ok"].astype(bool)

    # 4) Obtiene el function code para generar los bloques, accede al data element 24
    df["function_code"] = None
    mask_1644 = df["mti"].eq("1644") & df["parse_ok"].eq(True)
    
    if mask_1644.any():
        sub = df.loc[mask_1644, ["body", "bitmap", "enc", "fields"]]
        df.loc[mask_1644, "function_code"] = [
                extract_de24_fast(
                    body_hex=b, bitmap_hex=bm, enc=e, de_spec=DE_SPEC, fields=f)
                for b, bm, e, f in zip(
                    sub["body"].values, 
                    sub["bitmap"].values, 
                    sub["enc"].values, 
                    sub["fields"].values)
            ]

    #5) Genera los bloques de acuerdo al function code y message type
    df = add_block_column(df)

    #6) Generar el dataframe final y obtiene los dataelements de acuerdo al bitmap y body
    BATCH_SIZE = 50000  # 20 000
    schema = _canonical_schema_from_de_spec(DE_SPEC)
    writers: dict = {}  # key: (file_id, block, mti) -> ParquetWriter 

    n = len(df)
    for start in range(0, n, BATCH_SIZE):
        base_chunk = df.iloc[start:start + BATCH_SIZE]
        base_chunk = base_chunk[base_chunk["block"].notna()]
        if base_chunk.empty:
            continue

        wide_rows = []
        for r in base_chunk.itertuples(index=False, name="Msg"):
            wide_rows.append(
                build_wide_row(
                    msg_no=cast(int,r.msg_no),
                    block=cast(int,r.block),
                    mti=cast(Optional[str], r.mti),
                    enc=cast(Optional[str],r.enc),
                    function_code=cast(Optional[str],r.function_code),
                    function_role=getattr(r, "function_role", None),
                    parse_ok=cast(bool, r.parse_ok),
                    bitmap_hex=cast(Optional[str], r.bitmap),
                    body_hex=cast(Optional[str], r.body),
                    de_spec=DE_SPEC,
                    fields=cast(Optional[list[int]], r.fields),
                )
            )

        df_wide_chunk = pd.DataFrame(wide_rows)
        del wide_rows

        df_wide_chunk = df_wide_chunk.reindex(columns=schema.names)
        df_wide_chunk["msg_no"] = df_wide_chunk["msg_no"].astype("int64")
        df_wide_chunk["block"] = df_wide_chunk["block"].astype("int64")
        df_wide_chunk["parse_ok"] = df_wide_chunk["parse_ok"].astype(bool)

        write_parquet_by_mti_block_streaming(
            df_chunk=df_wide_chunk, fs=fs, target_layer=target_layer, 
            client_id=client_id, file_id=file_id, schema=schema, writers=writers)

        # libera DF del chunk
        del df_wide_chunk

    finalize_writers(writers=writers)
