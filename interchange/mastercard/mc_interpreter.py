import csv
from  pathlib import Path
import io
from typing import BinaryIO, Optional, cast, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage

from interchange.mastercard.interpreter.io.unblock import unblock_1014
from interchange.mastercard.interpreter.io.message_reader import (
    read_len_prefixed_messages, read_len_prefixed_messages_variable
    )

from interchange.mastercard.interpreter.iso8583.dataelements import Parameters
from interchange.mastercard.interpreter.iso8583.parse_format import (
    build_wide_row, 
    extract_de24_fast, 
    add_headers_fields_697, 
    apply_block_file_context_697
)

from interchange.mastercard.interpreter.storage.classified_block_mti import (
    write_parquet_by_mti_block_streaming,
    _canonical_schema_from_de_spec,
    finalize_writers,
)

log = Logger(__name__)
fs = FileStorage()
DE_SPEC = Parameters().getdataelements()


##############################################


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

def build_block_state_from_headers_695(
    df: pd.DataFrame,
    *,
    schema: pa.Schema,
    de_spec,
) -> dict[int, tuple[str, str]]:
    block_state: dict[int, tuple[str, str]] = {}

    fc = df["function_code"].astype("string").str.strip()
    hdr_idx = df.index[fc.eq("695")].to_list()
    if not hdr_idx:
        return block_state

    hdr_df = df.loc[hdr_idx]

    hdr_rows = []
    for r in hdr_df.itertuples(index=False, name="Msg"):
        hdr_rows.append(
            build_wide_row(
                msg_no=cast(int, r.msg_no),
                block=cast(int, r.block),
                mti=cast(Optional[str], r.mti),
                enc=cast(Optional[str], r.enc),
                function_code=cast(Optional[str], r.function_code),
                function_role=getattr(r, "function_role", None),
                parse_ok=cast(bool, r.parse_ok),
                bitmap_hex=cast(Optional[str], r.bitmap),
                body_hex=cast(Optional[str], r.body),
                de_spec=de_spec,
                fields=cast(Optional[list[int]], r.fields),
            )
        )

    hdr_wide = pd.DataFrame(hdr_rows).reindex(columns=schema.names)

    # llena file_idn/file_dt SOLO en headers
    add_headers_fields_697(hdr_wide)

    # construir state directo desde los headers
    h = (
        hdr_wide.loc[:, ["block", "file_idn", "file_dt"]]
        .dropna(subset=["block", "file_idn"])
        .copy()
    )
    if h.empty:
        return block_state

    h["block"] = h["block"].astype(int)

    # si hay varios 695 por block, toma el último
    h = h.drop_duplicates(subset=["block"], keep="last")

    block_state.update(
        dict(
            zip(
                h["block"].to_list(),
                zip(h["file_idn"].astype(str).to_list(), h["file_dt"].astype(str).to_list()),
            )
        )
    )
    return block_state

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

    need_interpreter_fix = db.needs_interpreter_fix(client_id=client_id, file_id=file_id)
    
    print(need_interpreter_fix)
    if need_interpreter_fix == True:
        rows = read_len_prefixed_messages(
            io.BytesIO(unblocked_bytes), 
            as_hex=False
        )
    elif need_interpreter_fix == False:
        rows = read_len_prefixed_messages_variable(
            io.BytesIO(unblocked_bytes),
            as_hex=False,
            encoding="cp500"
        )

    df = pd.DataFrame(rows)

    cols = ["msg_no", "offset", "msg_len", "mti", "enc", "parse_ok", "fields"]
    df_export = df.loc[:, [c for c in cols if c in df.columns]].copy()
    if "fields" in df_export.columns:
        df_export["fields"] = df_export["fields"].map(lambda x: str(x) if x is not None else "")

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
                    sub["fields"].values
                    )
            ]

    #5) Genera los bloques de acuerdo al function code y message type
    df = add_block_column(df)

    #6) Generar el dataframe final y obtiene los dataelements de acuerdo al bitmap y body
    BATCH_SIZE = 10000  # 20 000
    schema = _canonical_schema_from_de_spec(DE_SPEC)

    writers: dict = {}  # key: (file_id, block, mti) -> ParquetWriter 
  
    n = len(df)
   
    block_state = build_block_state_from_headers_695(df, schema=schema, de_spec=DE_SPEC)
    
    for start in range(0, n, BATCH_SIZE):
        base_chunk = df.iloc[start:start+BATCH_SIZE]
        base_chunk = base_chunk[base_chunk["block"].notna()]

        if base_chunk.empty:
            continue
        
        wide_rows = []
        none_ct = 0  #  
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

        add_headers_fields_697(df_wide_chunk)
        #Actualiza estado last_by_block con headers del chunk (vectorizado)
        apply_block_file_context_697(df_wide_chunk,state=block_state,strict=False,) # o True si quieres romper ante errores)

        # escribe / clasifica este bloque por el chunk obtenido
        
        write_parquet_by_mti_block_streaming(
            df_chunk=df_wide_chunk, fs=fs, target_layer=target_layer, 
            client_id=client_id, file_id=file_id, schema=schema, writers=writers)

        # libera DF del chunk
        del df_wide_chunk

    finalize_writers(writers=writers)


