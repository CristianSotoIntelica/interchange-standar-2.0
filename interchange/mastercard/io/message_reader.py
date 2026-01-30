from interchange.mastercard.iso8583.detect_mti import detect_mti
from interchange.mastercard.iso8583.split_mti import split_mti_bitmap_body

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