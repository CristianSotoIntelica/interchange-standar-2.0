from interchange.mastercard.iso8583.detect_mti import detect_mti
from interchange.mastercard.iso8583.split_mti import split_mti_bitmap_body

import struct

def read_len_prefixed_messages(stream):
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
            break

        msg_len = struct.unpack(">i", raw_len)[0]
        if msg_len <= 0:
            break

        payload = stream.read(msg_len)
        if len(payload) < msg_len:
            break

        msg_no += 1
        mti, enc = detect_mti(payload)

        parts = split_mti_bitmap_body(payload)
        if parts is None:
            rows.append({
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "parse_ok": False,
                "bitmap_hex": None,
                "body_hex": payload.hex(),
            })
        else:
            mti_bytes, bitmap, body, fields, has_secondary = parts
            rows.append({
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "parse_ok": True,
                "bitmap_hex": bitmap.hex(),
                "body_hex": body.hex(),
            })

        pos += 4 + msg_len

    return rows