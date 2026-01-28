from __future__ import annotations
from typing import Dict, Optional, Any
from collections.abc import Set as AbstractSet

from interchange.mastercard.iso8583.split_mti import bitmap_bits
from interchange.mastercard.iso8583.decode_digits import decode_digits

DEFAULT_NUMERIC_DES = frozenset({
    2,3,4,5,6,9,10,12,14,23,24,25,26,30,37,38,49,50,51,71,73,93,94,95,100
})
DEFAULT_BINARY_DES = frozenset({55})
DEFAULT_EBCDIC_TEXT_DES = frozenset({43, 22})  # 43 seguro; 22 según lo que viste (c3/c4)

DE_COL = {de: f"de_{de}" for de in range(2, 129)}

def parse_des_one_pass(
        body: bytes, fields: list[int], enc: str, de_spec: dict, *, max_de: int = 128,
        ) -> Dict[int, bytes]:
    
    if not body or not fields:
        return {}

    pos = 0
    out: Dict[int, bytes] = {}

    de_get = de_spec.get

    for de in fields:
        if de < 2:
            continue
        if de > max_de:
            break
    
        cfg = de_get(de)
        if not cfg:
            break
        
        length = cfg["length"]

        if cfg["fixed"]:
            ln = int(length)

            if pos + ln > len(body):
                break
            raw = body[pos:pos + ln]
            pos = pos + ln
        else:
            len_digits = int(length)

            if pos + len_digits > len(body):
                break

            raw_len = body[pos:pos + len_digits]
            pos = pos + len_digits

            try:
                ln = int(decode_digits(raw_len, enc).strip())
            except ValueError:
                break

            if pos + ln > len(body):
                break
            raw = body[pos:pos + ln]
            pos = pos + ln

        out[de] = raw
    return out


def decode_text_best(raw: bytes, enc: str) -> str:
    """
    si el MTI fue EBCDIC_DIGITS, cp500.
    """
    if enc == "EBCDIC_DIGITS":
        return raw.decode("cp500", errors="replace")
    try:
        return raw.decode("ascii", errors="replace")
    except UnicodeDecodeError:
        return raw.decode("latin1", errors="replace")


def format_de_value(
    de: int,
    raw: Optional[bytes],
    enc: str,
    *,
    numeric_des: AbstractSet[int] = DEFAULT_NUMERIC_DES,
    binary_des: AbstractSet[int] = DEFAULT_BINARY_DES,
    ebcdic_text_des: AbstractSet[int] = DEFAULT_EBCDIC_TEXT_DES,
) -> Optional[str]:
    if raw is None:
        return None

    if de in binary_des:
        return raw.hex()

    if de in numeric_des:
        return decode_digits(raw, enc).strip()      

    return decode_text_best(raw, enc)


def build_wide_row(
        *, msg_no: int, block: Optional[int], mti: Optional[str], enc: Optional[str],
    function_code: Optional[str], function_role: Optional[str], parse_ok: bool,
    bitmap_hex: Optional[str], body_hex: Optional[str], de_spec: dict,
    fields: Optional[list[int]] = None,
    numeric_des: AbstractSet[int] = DEFAULT_NUMERIC_DES, 
    binary_des: AbstractSet[int] = DEFAULT_BINARY_DES, 
    ebcdic_text_des: AbstractSet[int] = DEFAULT_EBCDIC_TEXT_DES,
    unknown_mode: str = "skip", # "skip" | "hex" | "bytes"
):
    """
    Convierte un row base (con body_hex/bitmap_hex) a row wide con columnas de data elements
    """
    base = {
        "msg_no": msg_no,
        "block": block,
        "mti": mti,
        "enc": enc,
        "function_code": function_code,
        "function_role": function_role,
        "parse_ok": parse_ok,
    }

    # si no parsea o no hay datos, devuelve solo la base
    if (not parse_ok) or (body_hex is None) or (not enc) or (bitmap_hex is None):
        return base

    if isinstance(body_hex, (bytes, bytearray)):
        body = bytes(body_hex)
    elif isinstance(body_hex, str):
        body = bytes.fromhex(body_hex)
    else:
        return base 

    if isinstance(bitmap_hex, (bytes, bytearray)):
        bitmap = bytes(bitmap_hex)
    elif isinstance(bitmap_hex, str):
        bitmap = bytes.fromhex(bitmap_hex)
    else:
        return base

    if fields is None:
        fields = bitmap_bits(bitmap=bitmap)

    # raw_map = parse_des_one_pass(body=body, fields=fields, enc=enc, de_spec=de_spec, max_de=128)

    pos = 0 
    de_get = de_spec.get
    cols = DE_COL

    num = numeric_des
    bin_ = binary_des
    txt_ = ebcdic_text_des 

    for de in fields:
        if de < 2:
            continue
        if de > 128:
            break

        cfg = de_get(de)
        if not cfg:
            break

        length = int(cfg["length"])

        if cfg["fixed"]:
            ln = length
        else:
            if pos + length > len(body):
                break
            raw_len = body[pos:pos + length]
            pos = pos + length
            try:
                ln = int(decode_digits(raw_len, enc).strip())
            except ValueError:
                break

        if pos + ln > len(body):
            break

        raw = body[pos:pos + ln]
        pos = pos + ln

        col = cols[de]

        if de in bin_:
            base[col] = raw.hex()
        elif de in num:
            base[col] = decode_digits(raw, enc).strip()
        elif de in txt_:
            base[col] = decode_text_best(raw, enc)
        else:
            if unknown_mode == "hex":
                base[col] = raw.hex()
            elif unknown_mode == "bytes":
                base[col] = raw
    return base

def extract_de24_fast(
        body_hex: Any, bitmap_hex: Any, enc: Any, de_spec: dict, 
        fields: Optional[list[int]]) -> str | None:
    
    if (body_hex is None) or (bitmap_hex is None) or (not enc):
        return None

    if isinstance(body_hex, (bytes, bytearray)):
        body = bytes(body_hex)
    else:
        body = bytes.fromhex(body_hex)

    if isinstance(bitmap_hex, (bytes, bytearray)):
        bitmap = bytes(bitmap_hex)
    else:
        bitmap = bytes.fromhex(bitmap_hex)

    if fields is None:
        fields = bitmap_bits(bitmap)

    # Splitear los DE en formato HEX
    raw_map = parse_des_one_pass(body=body, fields=fields, enc=enc, de_spec=de_spec, max_de=24) 
    
    raw24 = raw_map.get(24)
    if raw24 is None:
        return None

    return decode_digits(raw24, enc).strip()