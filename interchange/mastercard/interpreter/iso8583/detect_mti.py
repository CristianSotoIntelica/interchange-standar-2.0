
def detect_mti(
    payload: bytes, 
    encoding: str,
):
    """

    Esta función intenta adivinar si un mensaje empieza con un MTI válido (Message Type Indicator) - ISO-8583
    Mira los primeros 4 bytes del mensaje (payload) y verifica cómo están codificados.

    - ASCII dígitos '0'..'9'
    - EBCDIC dígitos F0..F9

    Devuelve: (mti_str, 'ASCII' o 'EBCDIC_DIGITS') o (None, None)

    ASCII : números y letras “normales”

    Se usa en la mayoría de archivos modernos

    EBCDIC_DIGITS números en formato mainframe IBM
    Muy usado en bancos, Visa/Mastercard legacy
    """
    
    if len(payload) < 4:
        return None, None
    
    m4 = payload[:4] 
    if encoding.upper() in ("LATIN-1", "LATIN1", "ISO-8859-1", "ASCII"):
        return m4.decode("ascii"), "ASCII"
    elif encoding.upper() in ("CP500", "EBCDIC", "EBCDIC_DIGITS"):
        return "".join(str(b - 0xF0) for b in m4), "EBCDIC_DIGITS"
    else:
        return None, None
