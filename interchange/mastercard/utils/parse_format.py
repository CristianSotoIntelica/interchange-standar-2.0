from __future__ import annotations
from typing import Dict, Optional, Set, Any
import binascii

from interchange.mastercard.utils.decode_digits import decode_digits

DEFAULT_NUMERIC_DES: Set[int] = {
    2,3,4,5,6,9,10,12,14,23,24,25,26,30,37,38,49,50,51,71,73,93,94,95,100
}
DEFAULT_BINARY_DES: Set[int] = {55}
DEFAULT_EBCDIC_TEXT_DES: Set[int] = {43, 22}  # 43 seguro; 22 según lo que viste (c3/c4)


def parse_des_one_pass(
        body: bytes, fields: list[int], enc: str, de_spec: dict, *, max_de: int = 128,
        ) -> Dict[int, bytes]:
    
    if not body or not fields:
        return {}

    present = sorted(f for f in fields if 2 <= f <= max_de)
    pos = 0
    out: Dict[int, bytes] = {}

    for de in present:
        cfg = de_spec.get(de)
        if not cfg:
            break

        if cfg["fixed"]:
            ln = int(cfg["length"])
            if pos + ln > len(body):
                break
            raw = body[pos:pos + ln]
            pos += ln
        else:
            len_digits = int(cfg["length"])  # 2 o 3 (LLVAR/LLLVAR)
            if pos + len_digits > len(body):
                break

            raw_len = body[pos:pos + len_digits]
            pos += len_digits

            ln_str = decode_digits(raw_len, enc).strip()
            if not ln_str.isdigit():
                break
            ln = int(ln_str)

            if pos + ln > len(body):
                break
            raw = body[pos:pos + ln]
            pos += ln

        out[de] = raw

    return out


def decode_text_best(raw: bytes, enc: str) -> str:
    """
    si el MTI fue EBCDIC_DIGITS, cp500.
    """
    if enc == "EBCDIC_DIGITS":
        return raw.decode("cp500", errors="replace").strip()
    try:
        return raw.decode("ascii").strip()
    except UnicodeDecodeError:
        return raw.decode("latin1", errors="replace").strip()


def format_de_value(
    de: int,
    raw: Optional[bytes],
    enc: str,
    *,
    numeric_des: Set[int] = DEFAULT_NUMERIC_DES,
    binary_des: Set[int] = DEFAULT_BINARY_DES,
    ebcdic_text_des: Set[int] = DEFAULT_EBCDIC_TEXT_DES,
) -> Optional[str]:
    if raw is None:
        return None

    if de in binary_des:
        return binascii.b2a_hex(raw).decode("ascii")

    if de in numeric_des:
        return decode_digits(raw, enc).strip()

    # if de in ebcdic_text_des:
    #     return raw.decode("cp500", errors="replace").strip()

    return decode_text_best(raw, enc)


def build_wide_row(
    *,
    msg_no: int,
    block: Optional[int],
    mti: Optional[str],
    enc: Optional[str],
    function_code: Optional[str],
    function_role: Optional[str],
    parse_ok: bool,
    bitmap_hex: Optional[str],
    body_hex: Optional[str],
    de_spec: dict,
    numeric_des: Set[int] = DEFAULT_NUMERIC_DES,
    binary_des: Set[int] = DEFAULT_BINARY_DES,
    ebcdic_text_des: Set[int] = DEFAULT_EBCDIC_TEXT_DES,
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

    if not parse_ok or not isinstance(body_hex, str) or not isinstance(bitmap_hex, str) or not enc:
        # deja columnas DE_... como None igual (para consistencia)
        for de in sorted(de_spec.keys()):
            base[f"de_{de}"] = None
        return base

    body = bytes.fromhex(body_hex)
    bitmap = bytes.fromhex(bitmap_hex)

    # IMPORTANTE: bitmap_bits está en split_mti.py, lo importamos aquí para que interpreter quede limpio
    from interchange.mastercard.utils.split_mti import bitmap_bits
    fields = bitmap_bits(bitmap)

    raw_map = parse_des_one_pass(body=body, fields=fields, enc=enc, de_spec=de_spec, max_de=128)

    for de in sorted(de_spec.keys()):
        raw = raw_map.get(de)
        base[f"de_{de}"] = format_de_value(
            de, raw, enc,
            numeric_des=numeric_des,
            binary_des=binary_des,
            ebcdic_text_des=ebcdic_text_des,
        )
    for de in sorted(de_spec.keys()):
        k = f"de_{de}"
        v = base.get(k)
        base[k] = None if v is None else str(v)

    return base

def extract_de24_fast(
        body_hex: Any, bitmap_hex: Any, enc: Any, de_spec: dict) -> str | None:
    
    if not body_hex or not bitmap_hex or not enc:
        return None

    body = bytes.fromhex(body_hex)
    bitmap = bytes.fromhex(bitmap_hex)

    from interchange.mastercard.utils.split_mti import bitmap_bits
    fields = bitmap_bits(bitmap)

    # Splitear los DE en formato HEX
    raw_map = parse_des_one_pass(body=body, fields=fields, enc=enc, de_spec=de_spec, max_de=24) 
    
    raw24 = raw_map.get(24)
    if raw24 is None:
        return None

    return decode_digits(raw24, enc).strip()