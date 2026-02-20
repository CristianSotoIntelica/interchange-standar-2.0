from interchange.mastercard.interpreter.iso8583.detect_mti import detect_mti
from interchange.mastercard.interpreter.iso8583.split_mti import split_mti_bitmap_body
import interchange.mastercard.interpreter.iso8583.dataelements as de


import struct

from typing import BinaryIO, List, Dict, Any


def _read_len_prefixed_messages(stream, *, as_hex: bool =True):
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
            #print("SE HIZO BREAK 0")
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
            # print("SE HIZO BREAK 1")
            # # Posible desincronización
            # # Igualmente leer los 4 siguientes bytes para capturar el posible MTI 
            # # incorrecto e imprirmirlo.
            # # Los MTI de esos 4 bytes son: 1240, 1442, 1644 y 1740.
            # # En caso que no sea uno de estos. Posiblemente sea poruqe requiere 
            # # sincronizarse de nuevo. 
            # # Es decir, recorrer los bytes hasta encontrar un grupo de 4 bytes que 
            # # tengan el valor de MTIs permitidos: 1240, 1442, 1644, 1740. 
            # possible_mti = stream.read(4)
            # print(f"possible: mti {possible_mti}")
            # stream.seek(stream.tell() - 4)
            break

        # possible_mti = stream.read(4)
        # print(f"possible: mti {possible_mti}")
        stream.seek(stream.tell() - 4)

        payload = stream.read(msg_len)
        if len(payload) < msg_len:
            # print("SE HIZO BREAK 2")
            # print(f"len del payload: {len(payload)} - len del msg {msg_len}")
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

def _bitmap_to_fields_1_128(bitmap_16: bytes) -> List[int]:
    fields = []
    de_no = 1
    for byte in bitmap_16:
        for bit in range(7, -1, -1):
            if (byte >> bit) & 1:
                fields.append(de_no)
            de_no += 1
    return fields


def read_len_prefixed_messages(
    stream: BinaryIO,
    *,
    as_hex: bool = False
) -> List[Dict[str, Any]]:
    """
    Reader estilo estructura ISO, no depende del msg_len:
      - Lee 4 bytes length (solo control)
      - Lee 20 bytes (MTI 4 + bitmap 16)
      - Lee DEs según bitmap y parameters (fixed/variable) con encoding cp500

    Retorna rows con bitmap/body (bytes o hex).
    """
    parameters = de.Parameters().getdataelements()

    rows = []
    msg_no = 0
    base0 = stream.tell()

    while True:

        msg_start = stream.tell()

        raw_len = stream.read(4)
        if len(raw_len) < 4:
            break

        try:
            record_length = struct.unpack(">i", raw_len)[0]  
        except Exception:
            record_length = 0

        if record_length == 0:
            break

        message_total = stream.read(20)
        if len(message_total) != 20:
            break

        mti_bytes, bitmap_16 = struct.unpack("4s16s", message_total)

        mti, enc, encoding = detect_mti(mti_bytes)  # robusto para EBCDIC digits

        fields_present = _bitmap_to_fields_1_128(bitmap_16)

        body_bytes = bytearray()

        parse_ok = True
        
        for i in range(2, 129):
            if i not in fields_present:
                continue

            if parameters[i]["fixed"]:
                de_len = parameters[i]["length"]
                v = stream.read(de_len)
                if len(v) < de_len:
                    break
                body_bytes.extend(v)

            else:
                len_digits = parameters[i]["length"]  # 2 o 3 usualmente
                raw_num = stream.read(len_digits)
                if len(raw_num) < len_digits:
                    break

                # en cp500 los dígitos vienen como F0..F9 y decode('cp500') da '0'..'9'
                try:
                    de_len = int(raw_num.decode(encoding))
                except Exception:
                    # si falla, cortamos como haría legacy con ValueError
                    de_len = 0

                v = stream.read(de_len)
                if len(v) < de_len:
                    parse_ok = False
                    break

                # si quieres que el "body" sea exactamente lo que venía (incluyendo el length prefix del campo),
                # mantenemos raw_num + valor
                body_bytes.extend(raw_num)
                body_bytes.extend(v)

        msg_no += 1
        
        has_secondary = (bitmap_16[0] & 0x80) != 0  # bit 1 del bitmap indica bitmap secundario
        
        row: Dict[str, Any] = {
            "msg_no": msg_no,
            "offset": msg_start - base0,
            "record_len": record_length,
            "mti": str(mti),
            "enc": enc,
            "parse_ok": parse_ok,
            "fields": fields_present,
            "has_secondary": has_secondary,
        }

        if as_hex:
            row["bitmap_hex"] = bitmap_16.hex()
            row["body_hex"] = bytes(body_bytes).hex()
        else:
            row["bitmap"] = bitmap_16
            row["body"] = bytes(body_bytes)

        rows.append(row)

    return rows