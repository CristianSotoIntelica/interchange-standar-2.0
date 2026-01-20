import io
import numpy as np
import pandas as pd
from pathlib import Path
from enum import Enum
from typing import BinaryIO, Optional

from interchange.logs.logger import Logger
from interchange.persistence.file import FileStorage

from interchange.mastercard.utils.unblock import unblock_1014
from interchange.mastercard.utils.dataelements import Parameters
from interchange.mastercard.utils.message_reader import read_len_prefixed_messages
from interchange.mastercard.utils.parse_format import build_wide_row
from interchange.mastercard.utils.parse_format import extract_de24_fast
from interchange.mastercard.utils.classified_block_mti import classified_block_mti_parts
from interchange.mastercard.utils.classified_block_mti import compact_parquet_parts



PROJECT_ROOT = Path(__file__).resolve().parent 
PATH_LOG = PROJECT_ROOT / "log_test" 
PATH_PERSISTENCE = Path(__file__).resolve().parent.parent / "persistence" 
PATH_STAGING = PATH_PERSISTENCE / "files" / "staging" / "SBSA"

log = Logger(__name__)
fs = FileStorage()

DE_SPEC = Parameters().getdataelements()

def add_block_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    is_header = df["function_code"].eq("697") & df["mti"].eq("1644")
    df["block"] = is_header.cumsum()
    df.loc[df["block"].eq(0), "block"] = np.nan
    return df


# def write_df_csv(df: pd.DataFrame, out_dir: str = "out", filename: str = "dataset_full.csv") -> str:
#     out_path = Path(out_dir)
#     out_path.mkdir(parents=True, exist_ok=True)
#     file_path = out_path / filename
#     df.to_csv(file_path, index=False, encoding="utf-8")
#     log.logger.info(f"[OK] CSV guardado en {file_path.resolve()}")
#     return str(file_path)

def _load_as_binary(layer: FileStorage.Layer, client_id: str, file_id: str, subdir="", test_path: str = "") -> BinaryIO:
    return fs.read_binary(fs.Layer.LANDING, client_id, file_id, subdir, True, test_path)

def interpretate_msg(origin_layer, target_layer, client_id: str, file_id: str, origin_subdir="", target_sub_dir="", test_path: str = "") -> None:
    
    # 1) Leer el archivo binario
    stream_file = _load_as_binary(origin_layer, client_id, file_id, subdir=origin_subdir, test_path=test_path)

    # 2) Elimina los bloqueantes
    unblocked_bytes = unblock_1014(stream_file=stream_file,payload_size=1012,sep_size=2, valid_seps=(b"", b"\x20\x20", b"\x40\x40"),)

    # 3) Lee nuevamente al archivo binario nuevo, delvuele un arreglo de body/bitmap en HEX con su message type y lo guarda en un DF
    rows = read_len_prefixed_messages(io.BytesIO(unblocked_bytes))
    #rows = read_len_prefixed_messages(stream_file)
    df = pd.DataFrame(rows)

    # 3) Obtiene el function code para generar los bloques, accede al data element 24
    mask_1644 = df["mti"].eq("1644") & df["parse_ok"].eq(True)

    df["function_code"] = None

    idx = df.index[mask_1644]
    df.loc[idx, "function_code"] = [
        extract_de24_fast(
            body_hex=df.at[i, "body_hex"],
            bitmap_hex=df.at[i, "bitmap_hex"],
            enc=df.at[i, "enc"],
            de_spec=DE_SPEC,
        )
        for i in idx
    ]

    #4) Genera los bloques de acuerdo al function code y message type
    df = add_block_column(df)

    #5) Generar el dataframe final y obtiene los dataelements de acuerdo al bitmap y body
    BATCH_SIZE = 2000  

    records = df.to_dict("records")

    for i in range(0, len(records), BATCH_SIZE):
        chunk = records[i : i + BATCH_SIZE]

        df_wide_chunk = pd.DataFrame([
            build_wide_row(
                msg_no=r.get("msg_no"),
                block=r.get("block"),
                mti=r.get("mti"),
                enc=r.get("enc"),
                function_code=r.get("function_code"),
                function_role=r.get("function_role"),
                parse_ok=r.get("parse_ok", False),
                bitmap_hex=r.get("bitmap_hex"),
                body_hex=r.get("body_hex"),
                de_spec=DE_SPEC,
            )
            for r in chunk
        ])

        # escribe / clasifica este bloque por el chunk obtenido
        classified_block_mti_parts(df_wide_chunk, PATH_STAGING, part_id=i // BATCH_SIZE)

        # libera memoria explícitamente
        del df_wide_chunk


    compact_parquet_parts(PATH_STAGING, de_spec=DE_SPEC)