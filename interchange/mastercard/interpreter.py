import pandas as pd 
from pathlib import Path
import numpy as np

import io
from enum import Enum
from interchange.logs.logger import Logger 
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage
from typing import BinaryIO, Optional
from interchange.mastercard.utils.unblock import unblock_1014
from interchange.mastercard.utils.detect_mti import detect_mti
from interchange.mastercard.utils.split_mti import split_mti_bitmap_body
from interchange.mastercard.utils.decode_digits import decode_digits
from interchange.mastercard.utils.dataelements import Parameters

import io

log = Logger(__name__)
fs = FileStorage()


MTIS = {"1240", "1442", "1644", "1740"}

# Spec de Data Elements (fixed / variable) para parsear el body
DE_SPEC = Parameters().getdataelements()

class FunctionRole(Enum):
    HEADER = "HEADER"
    TRAILER = "TRAILER"

def write_df_csv(df: pd.DataFrame, out_dir: str = "out", filename: str = "dataset_full.csv") -> str:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    file_path = out_path / filename

    # Opcional: ordena columnas para que quede “validable”
    preferred_cols = [
        "msg_no", "offset", "msg_len",
        "mti", "enc",
        "function_code", "function_role", "de24_raw_hex",
        "parse_ok",
        "bitmap_hex", "body_hex"
    ]
    cols = [c for c in preferred_cols if c in df.columns] + [c for c in df.columns if c not in preferred_cols]
    df_out = df[cols]

    df_out.to_csv(file_path, index=False, encoding="utf-8")

    log.logger.info(f"[OK] CSV guardado en {file_path.resolve()}")
    return str(file_path)

def add_block_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # True en filas header
    is_header = df["function_code"].eq("697") & df["mti"].eq("1644")

    # bloque incremental: 1,2,3...
    df["block"] = is_header.cumsum()

    # opcional: si quieres NaN antes del primer header:
    df.loc[df["block"].eq(0), "block"] = np.nan

    return df

def _load_as_ctf(
    layer: FileStorage.Layer, client_id: str, file_id: str, subdir="", 
    test_path: str = "") -> BinaryIO:

    stream_file = fs.read_binary(
        fs.Layer.LANDING, client_id, file_id, subdir, True, test_path)

    return stream_file

def extract_de24(body: bytes, fields: list[int], enc: str) -> Optional[str]:
    """
    Extrae DE24 consumiendo SOLO los DE presentes <= 24 (según fields).
    """
    if not body or not fields:
        return None

    fields_le24 = [f for f in fields if 2 <= f <= 24]  # DE1 no está en el body
    if 24 not in fields_le24:
        return None

    pos = 0

    # bitmap_bits() usualmente ya devuelve orden ascendente; si no, usa sorted(fields_le24)
    for de in fields_le24:
        cfg = DE_SPEC.get(de)
        if cfg is None:
            return None  # no sabemos avanzar seguro

        if cfg["fixed"]:
            ln = int(cfg["length"])
            if pos + ln > len(body):
                return None
            raw = body[pos : pos + ln]
            pos += ln
        else:
            len_digits = int(cfg["length"])  # 2=LLVAR, 3=LLLVAR
            if pos + len_digits > len(body):
                return None

            raw_len = body[pos : pos + len_digits]
            pos += len_digits

            len_str = decode_digits(raw_len, enc).strip()
            if not len_str.isdigit():
                return None
            ln = int(len_str)

            if pos + ln > len(body):
                return None
            raw = body[pos : pos + ln]
            pos += ln

        if de == 24:
            return decode_digits(raw, enc).strip(), raw.hex()

    return None

def function_role_from_1644(mti: str, function_code: Optional[str]) -> Optional[str]:
    """
    Interpretación semántica de DE24 para MTI 1644.
    """
    if mti != "1644" or not function_code:
        return None
    if function_code == "697":
        return FunctionRole.HEADER.value
    if function_code == "695":
        return FunctionRole.TRAILER.value
    return None

def split_stream_to_df_simple(stream_file: io.BytesIO) -> pd.DataFrame:
    """
    Parsea un stream con formato:
      [4 bytes length big-endian] + [payload length bytes] repetido.

    Devuelve DataFrame con mti, bitmap y body.
    Requiere tus funciones:
      - detect_mti(payload)
      - split_mti_bitmap_body(payload)
    """
    data = stream_file.getvalue()  # bytes completos

    rows = []
    pos = 0
    msg_no = 0

    while pos + 4 <= len(data):
        # 1) leer longitud
        msg_len = int.from_bytes(data[pos:pos + 4], "big", signed=False)

        # 2) va si el archivo está truncado o el len es 0/raro, paramos
        if msg_len <= 0 or (pos + 4 + msg_len) > len(data):
            break

        # 3) extraer payload
        payload = data[pos + 4:pos + 4 + msg_len]

        # 4) detectar MTI 
        mti, enc = detect_mti(payload)

        # 5) separar MTI/bitmap/body
        parts = split_mti_bitmap_body(payload)

        msg_no += 1

        if parts is None:
            # fallback: no pudo partir (payload raro), guardo payload completo
            rows.append({
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "bitmap_hex": None,
                "body_hex": payload.hex(),
                "parse_ok": False,
                "function_code": None,
                "function_role": None,
                "de24_raw_hex": None,
            })
        
        
        else:
            mti_bytes, bitmap, body, fields, has_secondary = parts

            function_code = None
            function_role = None
            de24_raw_hex = None

            if mti == "1644":
                 function_code, de24_raw_hex = extract_de24(body=body, fields=fields, enc=enc)
                 function_role = function_role_from_1644(mti, function_code)

            rows.append({
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "bitmap_hex": bitmap.hex(),
                "body_hex": body.hex(),
                "parse_ok": True,
                "function_code": function_code,
                "function_role": function_role,
                "de24_raw_hex": de24_raw_hex,

            })

        # 6) avanzar al siguiente mensaje
        pos = pos + 4 + msg_len

    return pd.DataFrame(rows)

def interpretate_msg(
    origin_layer: FileStorage.Layer, target_layer: FileStorage.Layer, client_id: str, 
    file_id: str, origin_subdir="", target_sub_dir="", test_path: str = "") -> None:

    """
    - Lee archivo binario
    - Desbloquea 1014 (1012 + 2 sep)
    - Parsea a DF y calcula DE24 para MTI 1644
    """
 
    valid_block_seps = (b"", b"\x20\x20", b"\x40\x40") # TODO: Deberia ser parametrizable en la BD
    payload_size = 1012 # TODO: Deberia ser parametrizable en la BD
    sep_size = 2 # TODO: Deberia ser parametrizable en la BD
    encoding = "latin1" # TODO: Validar si se usara

    stream_file = _load_as_ctf(origin_layer, client_id, file_id, 
                               subdir=origin_subdir, test_path= test_path)

    #unblocked_bytes = unblock_1014(
    #    stream_file=stream_file, payload_size=payload_size, sep_size=sep_size, 
    #    valid_seps=valid_block_seps) # Se podria parametrizar 

    #stream_file = io.BytesIO(unblocked_bytes)
    
    df = split_stream_to_df_simple(stream_file)
    df = add_block_column(df)
    
    #print(df.iloc[0:20])
    write_df_csv(df, out_dir="out", filename="dataset_full.csv")

    

