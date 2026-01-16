import pandas as pd 

from interchange.logs.logger import Logger 
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage
from typing import BinaryIO
from interchange.mastercard.utils.unblock import unblock_1014
from interchange.mastercard.utils.detect_mti import detect_mti
from interchange.mastercard.utils.split_mti import split_mti_bitmap_body
import io

########################################################################################
#DEBUG
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent
PATH_LOG = PROJECT_ROOT / "log_test"
########################################################################################

log = Logger(__name__)
fs = FileStorage()

MTIS = {"1240", "1442", "1644", "1740"}

def _load_as_binary(
    layer: FileStorage.Layer, client_id: str, file_id: str, subdir="", 
    test_path: str = "") -> BinaryIO:

    stream_file = fs.read_binary(
        fs.Layer.LANDING, client_id, file_id, subdir, True, test_path)

    return stream_file

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
            })
        else:
            mti_bytes, bitmap, body, fields, has_secondary = parts
            rows.append({
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "bitmap_hex": bitmap.hex(),
                "body_hex": body.hex(),
                "parse_ok": True,
            })

        # 6) avanzar al siguiente mensaje
        pos = pos + 4 + msg_len

    return pd.DataFrame(rows)

def interpretate_msg(
    origin_layer: FileStorage.Layer, target_layer: FileStorage.Layer, client_id: str, 
    file_id: str, origin_subdir="", target_sub_dir="", test_path: str = "") -> None:
    
    encoding = "latin1" # TODO: Validar si se usara

    stream_file = _load_as_binary(origin_layer, client_id, file_id, 
                               subdir=origin_subdir, test_path= test_path)

    unblocked_bytes = unblock_1014(
        stream_file=stream_file) # Se podria parametrizar 
    
    ####################################################################################
    # DEBUG
    debug_dir = PATH_LOG

    if test_path:
        input_name = Path(test_path).name
    else:
        input = f"{client_id}_{file_id}"
    
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    debug_file = debug_dir / f"log_{run_ts}_{input_name}_unblocked.txt"

    with open(debug_file, "wb") as f:
        f.write(unblocked_bytes)
    ####################################################################################

    stream_file = io.BytesIO(unblocked_bytes)
    
    df = split_stream_to_df_simple(stream_file)
    df_min = df.loc[df["parse_ok"], ["mti","msg_len","bitmap_hex", "body_hex", "enc"]]
    
    dfs_by_mti = {}

    for mti in MTIS:
        dfs_by_mti[mti] = (
            df_min[df_min["mti"] == mti]
            .reset_index(drop=True)
        )

    df_1240 = dfs_by_mti["1240"]
    df_1442 = dfs_by_mti["1442"]
    df_1644 = dfs_by_mti["1644"]
    df_1740 = dfs_by_mti["1740"]

    print(df_min.head())


    # Aca ya tenemos el archivo binario sin bloqueantes
    # Faltaria obtener por funciones el bitmap, el length, el body, msg_type ()
    # Faltaria clasificar los records binarios por tipo de mensaje
    # Faltaria generar parquets que acumulen los record de acuerdo al grupo de msg type: 
    # solo contendran
