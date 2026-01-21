from typing import Optional, Dict, Any
from interchange.mastercard.utils.decode_digits import decode_digits
# asumo que ya tienes esto:
# from interchange.mastercard.utils.decode_digits import decode_digits

def decode_numeric(text_bytes: bytes, enc: str) -> str:
    return decode_digits(text_bytes, enc).strip()

def decode_text_ebcdic(raw: bytes) -> str:
    try:
        return raw.decode("cp500").strip()
    except Exception:
        return raw.decode("latin1", errors="replace").strip()
    
def decode_text(raw: bytes, enc: str) -> str:
    if not raw:
        return ""

    # Si el MTI fue EBCDIC_DIGITS, en tu sistema “legacy” el texto suele ir en cp500
    if enc == "EBCDIC_DIGITS":
        try:
            return raw.decode("cp500", errors="replace").strip()
        except Exception:
            return raw.decode("latin1", errors="replace").strip()

    # Si es ASCII, intenta ascii y luego latin1
    try:
        return raw.decode("ascii").strip()
    except UnicodeDecodeError:
        return raw.decode("latin1", errors="replace").strip()

def extract_one_de_from_body(
    body: bytes,
    fields: list[int],
    enc: str,
    de_spec: dict,
    target_de: int,
    *,
    max_de: int = 128,
    numeric_des: Optional[set[int]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Extrae SOLO el DE target_de (si está presente en fields) consumiendo el body
    según de_spec, y lo decodifica (numeric vs text).
    Retorna dict: {"de": int, "raw": bytes, "raw_hex": str, "len": int, "text": str}
    o None si no se puede extraer.
    """
    if not body or not fields:
        return None

    if numeric_des is None:
        numeric_des = {
            2,3,4,5,6,9,10,12,14,22,23,24,25,26,30,37,38,40,41,42,
            49,50,51,71,73
        }

    # Solo DE presentes y en rango ISO (saltamos 1)
    present = sorted([f for f in fields if 2 <= f <= max_de])

    # Si el DE no está presente, no hay nada que extraer
    if target_de not in present:
        return None

    pos = 0
    n = len(body)

    for de in present:
        cfg = de_spec.get(de)
        if cfg is None:
            return None  # no sabemos avanzar seguro

        if cfg["fixed"]:
            ln = int(cfg["length"])
            if pos + ln > n:
                return None
            raw = body[pos:pos + ln]
            pos += ln
        else:
            len_digits = int(cfg["length"])  # 2 (LLVAR) o 3 (LLLVAR)
            if pos + len_digits > n:
                return None

            raw_len = body[pos:pos + len_digits]
            pos += len_digits

            ln_str = decode_numeric(raw_len, enc)
            if not ln_str.isdigit():
                return None
            ln = int(ln_str)

            if pos + ln > n:
                return None
            raw = body[pos:pos + ln]
            pos += ln

        # Cuando llegamos al target, devolvemos

        if de == target_de:
            text = decode_numeric(raw, enc) if de in numeric_des else decode_text(raw, enc)
            return {
                "de": de,
                "raw": raw,
                "raw_hex": raw.hex(),
                "len": len(raw),
                "text": text,
            }

    return None
