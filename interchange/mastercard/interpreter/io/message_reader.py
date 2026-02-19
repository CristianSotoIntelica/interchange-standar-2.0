from interchange.mastercard.interpreter.iso8583.detect_mti import detect_mti
from interchange.mastercard.interpreter.iso8583.split_mti import split_mti_bitmap_body
from interchange.logs.logger import Logger

import struct

def read_len_prefixed_messages(stream, *, as_hex: bool =True):
    """
    Lee [4 bytes len] + [payload]
    Devuelve rows con body/bitmap en HEX.
    """
    rows = []
    pos = 0
    msg_no = 0

    while True:
        raw_len = stream.read(4)
        if len(raw_len) < 4:
            print("SE HIZO BREAK 0")
            break

        msg_len = struct.unpack(">I", raw_len)[0]

        print(
        f"[DEBUG] raw_len_hex={raw_len.hex()} | "
        f"msg_len_signed={msg_len} | "
        f"msg_len_unsigned={struct.unpack('>I', raw_len)[0]} | "
        f"msg_len_2bytes={struct.unpack('>H', raw_len[:2])[0]} | "
        f"pos={pos} | next_msg_no={msg_no+1}"
        )


        if msg_len <= 0:
            print("SE HIZO BREAK 1")
            # Posible desincronización
            # Igualmente leer los 4 siguientes bytes para capturar el posible MTI 
            # incorrecto e imprirmirlo.
            # Los MTI de esos 4 bytes son: 1240, 1442, 1644 y 1740.
            # En caso que no sea uno de estos. Posiblemente sea poruqe requiere 
            # sincronizarse de nuevo. 
            # Es decir, recorrer los bytes hasta encontrar un grupo de 4 bytes que 
            # tengan el valor de MTIs permitidos: 1240, 1442, 1644, 1740. 
            possible_mti = stream.read(4)
            print(f"possible: mti {possible_mti}")
            stream.seek(stream.tell() - 4)
            break

        possible_mti = stream.read(4)
        print(f"possible: mti {possible_mti}")
        stream.seek(stream.tell() - 4)

        payload = stream.read(msg_len)
        if len(payload) < msg_len:
            print("SE HIZO BREAK 2")
            print(f"len del payload: {len(payload)} - len del msg {msg_len}")
            break

        msg_no += 1

        mti, enc = detect_mti(payload)

        print(f"mti: {mti} y el enc: {enc}" )

        parts = split_mti_bitmap_body(payload)
        if parts is None:
            row = {
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "parse_ok": False,
            }
            if as_hex:
                row["bitmap_hex"] = None
                row["body_hex"] = payload.hex()
            else:
                row["bitmap"] = None
                row["body"] = payload  # bytes (solo debug; no se parsea)
            rows.append(row)
        else:
            mti_bytes, bitmap, body, fields, has_secondary = parts
            row = {
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "parse_ok": True,
                "fields" : fields,
            }
            if as_hex:
                row["bitmap_hex"] = bitmap.hex()
                row["body_hex"] = body.hex()
            else:
                row["bitmap"] = bitmap
                row["body"] = body
            rows.append(row)

        pos += 4 + msg_len
    return rows

########################################################################################

import struct
from typing import BinaryIO, Iterable, Optional, Tuple, List

from interchange.mastercard.interpreter.iso8583.detect_mti import detect_mti
from interchange.mastercard.interpreter.iso8583.split_mti import split_mti_bitmap_body

log = Logger(__name__)

def _mti_ascii(mti: str) -> bytes:
    return mti.encode("ascii")


def _mti_ebcdic_digits(mti: str) -> bytes:
    # '0'..'9' -> F0..F9
    return bytes((0xF0 + int(ch)) for ch in mti)


def _build_mti_patterns(allowed_mtis: Iterable[str]) -> List[bytes]:
    pats: List[bytes] = []
    for m in allowed_mtis:
        pats.append(_mti_ascii(m))
        pats.append(_mti_ebcdic_digits(m))
    return pats


def _is_seekable(stream: BinaryIO) -> bool:
    try:
        cur = stream.tell()
        stream.seek(cur)
        return True
    except Exception:
        return False


def _stream_end_pos(stream: BinaryIO) -> Optional[int]:
    """Devuelve end position si se puede seekear; si no, None."""
    if not _is_seekable(stream):
        return None
    cur = stream.tell()
    try:
        stream.seek(0, 2)  # end
        end = stream.tell()
        return end
    finally:
        stream.seek(cur)


def _peek(stream: BinaryIO, n: int) -> bytes:
    """Lee n bytes sin avanzar el puntero (requiere seek)."""
    if not _is_seekable(stream):
        # Fallback: no peek real
        return b""
    cur = stream.tell()
    b = stream.read(n)
    stream.seek(cur)
    return b


def _find_next_mti_pos(
    stream: BinaryIO,
    patterns: List[bytes],
    *,
    scan_end_pos: int,
    chunk_size: int = 64 * 1024,
) -> Optional[int]:
    """
    Busca la próxima ocurrencia (más temprana) de cualquiera de los patterns (4 bytes)
    desde la posición actual del stream hasta scan_end_pos. Devuelve la posición absoluta
    donde empieza el MTI (mti_pos) o None.
    """
    tail = b""
    while True:
        cur = stream.tell()
        if cur >= scan_end_pos:
            return None

        to_read = min(chunk_size, scan_end_pos - cur)
        chunk = stream.read(to_read)
        if not chunk:
            return None

        data = tail + chunk

        best_idx = None
        for pat in patterns:
            i = data.find(pat)
            if i != -1 and (best_idx is None or i < best_idx):
                best_idx = i

        if best_idx is not None:
            # posición absoluta:
            # chunk empezó en (stream.tell() - len(chunk))
            chunk_start = stream.tell() - len(chunk)
            mti_pos = chunk_start - len(tail) + best_idx
            return mti_pos

        # conservar últimos 3 bytes para matches cruzando borde
        tail = data[-3:] if len(data) >= 3 else data


def _pick_valid_len(
    raw_len: bytes,
    *,
    min_payload_len: int,
    max_payload_len: int,
) -> Optional[int]:
    """
    Decide msg_len usando signed o unsigned, siempre dentro de rango.
    """
    msg_len_s = struct.unpack(">i", raw_len)[0]
    if min_payload_len <= msg_len_s <= max_payload_len:
        return msg_len_s

    msg_len_u = struct.unpack(">I", raw_len)[0]
    if min_payload_len <= msg_len_u <= max_payload_len:
        return msg_len_u

    return None


def _resync_to_next_len_prefix(
    stream: BinaryIO,
    *,
    patterns: List[bytes],
    allowed_mtis: Tuple[str, ...],
    start_pos: int,
    backtrack: int,
    max_scan_bytes: int,
    min_payload_len: int,
    max_payload_len: int,
    debug: bool = False,
) -> Optional[int]:
    """
    Busca hacia adelante el próximo MTI permitido (ASCII/EBCDIC), y cuando lo encuentra,
    intenta validar que 4 bytes antes hay un length válido y que el payload inicia con MTI permitido.
    Devuelve la posición del length prefix (len_pos) o None.
    """
    if not _is_seekable(stream):
        return None

    end_pos = _stream_end_pos(stream)
    scan_start = max(start_pos - backtrack, 0)

    if end_pos is None:
        scan_end = scan_start + max_scan_bytes
    else:
        scan_end = min(scan_start + max_scan_bytes, end_pos)

    stream.seek(scan_start)

    # Para evitar falsos positivos dentro del payload: cuando hallamos MTI, validamos len justo antes
    while True:
        mti_pos = _find_next_mti_pos(stream, patterns, scan_end_pos=scan_end)
        if mti_pos is None:
            return None

        len_pos = mti_pos - 4
        if len_pos < 0:
            # sigue buscando después del MTI encontrado
            stream.seek(mti_pos + 1)
            continue

        # validar length
        stream.seek(len_pos)
        raw_len = stream.read(4)
        if len(raw_len) < 4:
            return None

        msg_len = _pick_valid_len(
            raw_len, min_payload_len=min_payload_len, max_payload_len=max_payload_len
        )
        if msg_len is None:
            # falso positivo: sigue buscando después del MTI encontrado
            stream.seek(mti_pos + 1)
            continue

        # validar que inmediatamente después del len hay MTI permitido (ASCII/EBCDIC),
        # y además que detect_mti lo reconoce y está dentro de allowed_mtis
        stream.seek(len_pos + 4)
        mti_bytes = stream.read(4)
        if mti_bytes not in patterns:
            stream.seek(mti_pos + 1)
            continue

        # validación extra: detect_mti sobre esos 4 bytes (y su encoding)
        # (le damos 4 bytes + un mínimo de bitmap para que detect_mti sea consistente si quieres)
        stream.seek(len_pos + 4)
        prefix = stream.read(min(msg_len, 12))  # suficiente para MTI+bitmap mínimo
        mti_str, enc = detect_mti(prefix)
        if mti_str not in allowed_mtis:
            stream.seek(mti_pos + 1)
            continue

        # validar que el mensaje completo entra en el archivo (si sabemos end_pos)
        if end_pos is not None and (len_pos + 4 + msg_len) > end_pos:
            return None

        if debug:
            # deja el puntero listo para que el caller haga seek(len_pos)
            pass

        return len_pos


def _read_len_prefixed_messages(
    stream: BinaryIO,
    *,
    as_hex: bool = True,
    allowed_mtis: Tuple[str, ...] = ("1240", "1442", "1644", "1740"),
    min_payload_len: int = 12,          # 4 MTI + 8 bitmap mínimo
    max_payload_len: int = 2_000_000,   # ajusta a tu realidad (2MB ejemplo)
    resync: bool = True,
    resync_backtrack: int = 3,          # cubre desalineación -1..-3 bytes
    resync_max_scan_bytes: int = 10_000_000,  # 10MB de ventana de búsqueda (posibles lengths )
    debug: bool = True,
):
    """
    Lee [4 bytes len] + [payload] y devuelve rows.

    Si detecta un length inválido (<=0 o fuera de rango), intenta:
      1) usar unsigned si cae dentro de rango
      2) si no, resync: busca un MTI permitido (ASCII/EBCDIC) y retrocede 4 bytes para recuperar length
    """
    rows = []
    msg_no = 0

    patterns = _build_mti_patterns(allowed_mtis)

    while True:
        pos = stream.tell()
        raw_len = stream.read(4)
        if len(raw_len) < 4:
            break

        msg_len_s = struct.unpack(">i", raw_len)[0]
        msg_len_u = struct.unpack(">I", raw_len)[0]

        msg_len = _pick_valid_len(
            raw_len, min_payload_len=min_payload_len, max_payload_len=max_payload_len
        )

        if msg_len is None:
            # Debug: mira los 4 bytes siguientes al len inválido
            peek_mti = _peek(stream, 4)

            if debug:
                log.logger.debug(
                    f"[RESYNC] invalid_len at pos={pos} "
                    f"raw_len_hex={raw_len.hex()} signed={msg_len_s} unsigned={msg_len_u} "
                    f"next4={peek_mti.hex()} next4_ascii={peek_mti!r}"
                )
                # print(
                #     f"[RESYNC] invalid_len at pos={pos} "
                #     f"raw_len_hex={raw_len.hex()} signed={msg_len_s} unsigned={msg_len_u} "
                #     f"next4={peek_mti.hex()} next4_ascii={peek_mti!r}"
                # )

            if not resync:
                break

            new_len_pos = _resync_to_next_len_prefix(
                stream,
                patterns=patterns,
                allowed_mtis=allowed_mtis,
                start_pos=pos,
                backtrack=resync_backtrack,
                max_scan_bytes=resync_max_scan_bytes,
                min_payload_len=min_payload_len,
                max_payload_len=max_payload_len,
                debug=debug,
            )

            if new_len_pos is None:
                # no se pudo resync
                break

            # Reposiciona al inicio del nuevo length prefix y reintenta el loop
            stream.seek(new_len_pos)
            continue

        payload = stream.read(msg_len)
        if len(payload) < msg_len:
            break

        msg_no += 1

        mti, enc = detect_mti(payload)

        parts = split_mti_bitmap_body(payload)
        if parts is None:
            row = {
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "parse_ok": False,
            }
            if as_hex:
                row["bitmap_hex"] = None
                row["body_hex"] = payload.hex()
            else:
                row["bitmap"] = None
                row["body"] = payload
            rows.append(row)
        else:
            mti_bytes, bitmap, body, fields, has_secondary = parts
            row = {
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "parse_ok": True,
                "fields": fields,
            }
            if as_hex:
                row["bitmap_hex"] = bitmap.hex()
                row["body_hex"] = body.hex()
            else:
                row["bitmap"] = bitmap
                row["body"] = body
            rows.append(row)

    return rows
