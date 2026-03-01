import gc
import io
from typing import BinaryIO, Optional, cast

import pandas as pd
import pyarrow as pa
from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage

from interchange.mastercard.interpreter.io.unblock import unblock_1014
from interchange.mastercard.interpreter.io.message_reader import (
    read_len_prefixed_messages, read_len_prefixed_messages_variable
    )

from interchange.mastercard.interpreter.iso8583.detect_encoding import obtain_encoding

from interchange.mastercard.interpreter.iso8583.dataelements import Parameters
from interchange.mastercard.interpreter.iso8583.parse_format import (
    build_wide_row, 
    extract_de24_fast, 
    add_headers_fields_697
)

from interchange.mastercard.interpreter.storage.classified_block_mti import (
    write_parquet_by_mti_block_streaming,
    _canonical_schema_from_de_spec,
    finalize_writers,
)


log = Logger(__name__)
fs = FileStorage()
DE_SPEC = Parameters().getdataelements()

# FUNCION AUXILIAR 1
def _load_as_binary(
        layer: FileStorage.Layer, 
        client_id: str, 
        file_id: str, 
        subdir=""
) -> BinaryIO:
    return fs.read_binary(fs.Layer.LANDING, client_id, file_id, subdir, True)

# FUNCION AUXILIAR 2
def _extract_function_code_inline(
        row: dict,
        de_spec: dict
) -> Optional[str]:
    """
    Extrrae el function_code (DE24) directamente de un row del generador.
    Retorna None si el mensaje no es 1644 o no es válido.
    """
    if row.get("mti") != "1644" or not row.get("parse_ok", False):
        return None
    
    return extract_de24_fast(
        body_hex=row.get("body"),
        bitmap_hex=row.get("bitmap"),
        enc=str(row.get("enc", "")),
        de_spec=de_spec,
        fields=row.get("fields"),
    )

# FUNCION AUXILIAR 3
def _process_block(
        block_buffer: list,
        block_no: int,
        file_idn: Optional[str],
        file_dt: Optional[str],
        schema: pa.Schema,
        writers: dict,
        target_layer,
        client_id: str,
        file_id: str,
) -> None:
    """
    Convierte el buffer de un bloque a DataFrame wide, aplica el contexto 
    file_idn/file_dt del trailer 695, y escribe el parquet correspondiente.
    Se llama una vez por bloque, justo cuando se detecta el 695.
    """
    if not block_buffer:
        return 
    
    wide_rows = []
    for row in block_buffer:
        wide_rows.append(
            build_wide_row(
                msg_no=cast(int, row["msg_no"]),
                block=block_no,
                mti=cast(Optional[str], row.get("mti")),
                enc=cast(Optional[str], row.get("enc")),
                function_code=cast(Optional[str], row.get("function_code")),
                function_role=row.get("function_role"),
                parse_ok=cast(bool, row.get("parse_ok", False)),
                bitmap_hex=row.get("bitmap"),
                body_hex=row.get("body"),
                de_spec=DE_SPEC,
                fields=cast(Optional[list[int]], row.get("fields")),
            )
        )

    df_block = pd.DataFrame(wide_rows)
    del wide_rows  # libera la lista intermedia inmediatamente

    # Alinear columnas con el schema cannonical
    df_block = df_block.reindex(columns=schema.names)

    # Aplicar el contextgo del 695 a TODAS las filas del bloque
    # Este es el dato que antes requería la segunda pasada completa.
    if file_idn is not None:
        df_block["file_idn"] = file_idn 
        df_block["file_dt"] = file_dt
    
    # Escribir a parquet (clasificado por MTI)
    write_parquet_by_mti_block_streaming(
        df_chunk=df_block,
        fs=fs,
        target_layer=target_layer,
        client_id=client_id,
        file_id=file_id,
        schema=schema,
        writers=writers
    )

    del df_block # liberar el DF del bloque antes de retornar

def interpretate_msg(
        origin_layer, 
        target_layer, 
        client_id: str, 
        file_id: str, 
        origin_subdir="", 
        target_sub_dir="", 
        test_path: str = ""
) -> None:
    """
    Interpreta un archivo IPM Mastercard y escribe parquets clasificados por MTI y bloque.

    ALGORITMO (single-pass, bloque a bloque):
    ──────────────────────────────────────────
    En vez de leer todo el archivo en RAM y luego procesar (dos pasadas),
    ahora leemos mensaje a mensaje y mantenemos en RAM solo el bloque actual.

    Flujo por mensaje:
      - Si es 1644/697 (header): abre un nuevo bloque, inicia el buffer
      - Si es 1644/695 (trailer): extrae file_idn/file_dt, procesa el bloque
                                   completo, y LIMPIA el buffer → libera RAM
      - Cualquier otro mensaje: se agrega al buffer del bloque activo

    Resultado de RAM:
      ANTES: payloads de TODOS los mensajes simultaneamente (~2GB)
      AHORA: buffer de UN BLOQUE (~2-5MB típico)
    """

    # 1) Leer el archivo binario
    stream_file = _load_as_binary(
        origin_layer, 
        client_id, 
        file_id, 
        subdir=origin_subdir
    )

    # Conectar BD
    db = Database()

    # 2) Elimina los bloqueantes
    need_unblock = db.needs_unblock_for_file(client_id=client_id, file_id=file_id)
    # Validar si requiere el interpretador fijo (version 1) o interpretador variable (version 2)
    need_interpreter_fix = db.needs_interpreter_fix(client_id=client_id, file_id=file_id)
    file_mc_encoding = obtain_encoding(db=db, client_id=client_id, file_id=file_id)
    file_mc_encoding = str(file_mc_encoding)

    print(f"Type_encoding: {file_mc_encoding}")
    print(f"Need unblock?: {need_unblock}")
    print(f"need_intrepreter_fix: {need_interpreter_fix}")
    
    # 3) Lee nuevamente al archivo binario nuevo, delvuele un arreglo de body/bitmap en HEX con su message type y lo guarda en un DF
    if need_unblock:
        unblocked_bytes = unblock_1014(stream_file=stream_file) # ANALISIS: 300mb en RAM
    else:
        stream_file.seek(0)    
        unblocked_bytes = stream_file.read()

    stream_io = io.BytesIO(unblocked_bytes) 
    del unblocked_bytes # liberamos 300mb en este momento
    del stream_file # liberamos porque ya no lo usamos
    gc.collect()

    # 4) Obtener el generador de mensajes
    if need_interpreter_fix == True:
        rows = read_len_prefixed_messages( 
            stream=stream_io,
            as_hex=False,
            client_id=client_id,
            file_id=file_id,
            db=db,
            encoding=file_mc_encoding,
        )
    elif need_interpreter_fix == False:
        rows = read_len_prefixed_messages_variable(
            stream=stream_io,
            as_hex=False,
            client_id=client_id,
            file_id=file_id,
            db=db,
            encoding=file_mc_encoding
        )

    # 5) Procesar mensaje a mensaje, bloque por bloque
    schema = _canonical_schema_from_de_spec(DE_SPEC)
    writers: dict = {}

    current_block = 0 # numero del bloque que estamos procesando
    block_open = False # True entre 697 y 695
    block_buffer = [] # rows del bloque actual (se limpia al cerrar cada bloque)

    for row in rows: 
        # enriquecer el row con su function_code extraido inline
        row["function_code"] = _extract_function_code_inline(row=row, de_spec=DE_SPEC)

        mti = row.get("mti")
        fc = row.get("function_code")
        parse_ok = row.get("parse_ok", False)

        # HEADER = 697: abrir nuevo bloque
        if mti == "1644" and fc == "697" and parse_ok:
            current_block = current_block + 1 
            block_open = True
            block_buffer = [row] # inicia el buffer con el propio 697

        # TRAILER = 695: cierre el bloque y libera
        elif mti == "1644" and fc == "695" and parse_ok and block_open:
            block_buffer.append(row)   # el 695 también entra al buffer

            # Extraer file_idn / file_dt del DE48 del trailer 695.
            # Antes esto requería tener todos los rows en el df y hacer build_block_state.
            # Ahora lo hacemos sobre un único row, sin acumular nada extra.
            wide_695 = build_wide_row(
                msg_no=cast(int, row["msg_no"]),
                block=current_block,
                mti="1644",
                enc=cast(Optional[str], row.get("enc")),
                function_code="695",
                function_role=None,
                parse_ok=True,
                bitmap_hex=row.get("bitmap"),
                body_hex=row.get("body"),
                de_spec=DE_SPEC,
                fields=cast(Optional[list[int]], row.get("fields")),
            )
            df_695 = pd.DataFrame([wide_695])
            add_headers_fields_697(df_695)   # llena file_idn y file_dt en el df

            file_idn = str(df_695.at[0, "file_idn"]) if "file_idn" in df_695.columns else None
            file_dt  = str(df_695.at[0, "file_dt"])  if "file_dt"  in df_695.columns else None
            del df_695, wide_695
        
            # Procesar el bloque completo (buffer → parquet) y limpiar
            _process_block(
                block_buffer=block_buffer,
                block_no=current_block,
                file_idn=file_idn,
                file_dt=file_dt,
                schema=schema,
                writers=writers,
                target_layer=target_layer,
                client_id=client_id,
                file_id=file_id,
            )

            block_buffer.clear()   # ← RAM del bloque liberada aquí
            block_open = False
            pa.default_memory_pool().release_unused()

        # ── Cualquier otro mensaje: acumular en el buffer del bloque activo ───
        elif block_open:
            block_buffer.append(row)
       
    # ── PASO 6: Cleanup final ─────────────────────────────────────────────────
    del stream_io
    gc.collect()
    finalize_writers(writers=writers)