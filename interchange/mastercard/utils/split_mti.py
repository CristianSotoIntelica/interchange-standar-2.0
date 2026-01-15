
def bitmap_bits(bitmap: bytes) -> list[int]:
    """
    Devuelve la lista de campos presentes según el bitmap.
    - Si bitmap tiene 8 bytes: campos 1..64
    - Si bitmap tiene 16 bytes: campos 1..128

    Convención: bit más significativo (MSB) primero.
    """
    fields: list[int] = []
    for i, byte in enumerate(bitmap):
        for bit in range(8):
            if byte & (1 << (7 - bit)):
                fields.append(i * 8 + bit + 1)
    return fields


def split_mti_bitmap_body(payload: bytes):
    """
    Separa payload en:
      - mti_bytes (4)
      - bitmap_bytes (8 o 16)
      - body_bytes (resto)
      - fields_present (lista)
      - has_secondary (bool)

    Asume que payload empieza con MTI (4 bytes) seguido por bitmap (8 bytes).
    Si el bit 1 del bitmap primario está encendido, agrega bitmap secundario (8 bytes).
    """
    if len(payload) < 12:  # 4 MTI + 8 bitmap mínimo
        return None

    mti_bytes = payload[:4]
    primary = payload[4:12]  # 8 bytes

    # ¿Hay bitmap secundario? -> bit más alto del primer byte del primario
    has_secondary = bool(primary[0] & 0x80)

    if has_secondary:
        if len(payload) < 20:  # 4 + 8 + 8
            return None
        secondary = payload[12:20]
        bitmap = primary + secondary
        body = payload[20:]
    else:
        bitmap = primary
        body = payload[12:]

    fields = bitmap_bits(bitmap)
    return mti_bytes, bitmap, body, fields, has_secondary
