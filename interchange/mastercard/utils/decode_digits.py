def decode_digits(b: bytes, enc: str) -> str:
    """
    Convierte bytes que representan dígitos (ASCII o EBCDIC) a string.
    - ASCII: b'697' -> '697'
    - EBCDIC: 0xF0..0xF9 -> '0'..'9'
    """
    enc = (enc or "").lower()
    if "ebcdic" in enc:
        out = []
        for x in b:
            if 0xF0 <= x <= 0xF9:
                out.append(chr(ord("0") + (x - 0xF0)))
            else:
                out.append(chr(x))  # fallback defensivo
        return "".join(out)
    return b.decode("latin1", errors="ignore")