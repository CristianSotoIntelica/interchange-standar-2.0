from interchange.mastercard.interpreter.iso8583.detect_mti import detect_mti
from interchange.mastercard.interpreter.iso8583.split_mti import split_mti_bitmap_body
import interchange.mastercard.interpreter.iso8583.dataelements as de

from interchange.persistence.database import Database
from interchange.logs.logger import Logger
from typing import BinaryIO, List, Dict, Any, Optional
import struct

log = Logger(__name__)

def read_len_prefixed_messages(
    stream, 
    *, 
    as_hex: bool =True,
    client_id : str, 
    file_id: str, 
    db: Database,
    encoding: str,
):
    """
    Lee [4 bytes len] + [payload]
    Devuelve rows con body/bitmap en HEX.
    """
    rows = []
    pos = 0
    msg_no = 0

    # TODO: RETIRAR OBTENER ENCODING A PARTIR DEL DETECT_MTI. YA QUE 
    # EL DETECT_MTI GOLPEARA N VECCES A LA BD DE ACUERDO A LOS N ROWS QUE SE TENGAN
    # EXTRAER LA CONSULTA DE ENCODING PARA 
    while True:
        raw_len = stream.read(4)
        if len(raw_len) < 4:
            break

        msg_len = struct.unpack(">i", raw_len)[0]
        # print(f"raw_len: {raw_len}")
        # print(f"msg_len: {msg_len}")
        if msg_len <= 0:
            break

        payload = stream.read(msg_len)
        if len(payload) < msg_len:
            break

        msg_no = msg_no + 1 
        mti, enc = detect_mti(
            payload=payload,
            encoding=encoding
        )

        # print(f"mti: {mti}")

        parts = split_mti_bitmap_body(payload=payload)
        if parts is None:
            row = {
                "msg_no": msg_no,
                "offset": pos,
                "msg_len": msg_len,
                "mti": mti,
                "enc": enc,
                "parse_ok": False
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
        # print(rows)
        pos = pos + 4 + msg_len
    return rows


    # while True:
    #     raw_len = stream.read(4)
    #     if len(raw_len) < 4:
    #         break

    #     msg_len = struct.unpack(">i", raw_len)[0]
    #     if msg_len <= 0:
    #         break

    #     payload = stream.read(msg_len)
    #     if len(payload) < msg_len:
    #         break

    #     msg_no += 1
    #     mti, enc = detect_mti(
    #         payload=payload, 
    #         encoding=encoding
    #     )

    #     parts = split_mti_bitmap_body(payload)
    #     if parts is None:
    #         row = {
    #             "msg_no": msg_no,
    #             "offset": pos,
    #             "msg_len": msg_len,
    #             "mti": mti,
    #             "enc": enc,
    #             "parse_ok": False,
    #         }
    #         if as_hex:
    #             row["bitmap_hex"] = None
    #             row["body_hex"] = payload.hex()
    #         else:
    #             row["bitmap"] = None
    #             row["body"] = payload  # bytes (solo debug; no se parsea)
    #         rows.append(row)
    #     else:
    #         mti_bytes, bitmap, body, fields, has_secondary = parts
    #         row = {
    #             "msg_no": msg_no,
    #             "offset": pos,
    #             "msg_len": msg_len,
    #             "mti": mti,
    #             "enc": enc,
    #             "parse_ok": True,
    #             "fields" : fields,
    #         }
    #         if as_hex:
    #             row["bitmap_hex"] = bitmap.hex()
    #             row["body_hex"] = body.hex()
    #         else:
    #             row["bitmap"] = bitmap
    #             row["body"] = body
    #         rows.append(row)

    #     pos += 4 + msg_len
    # return rows

def _bitmap_to_fields_1_128(bitmap_16: bytes) -> List[int]:
    fields = []
    de_no = 1
    for byte in bitmap_16:
        for bit in range(7, -1, -1):
            if (byte >> bit) & 1:
                fields.append(de_no)
            de_no += 1
    return fields


def read_len_prefixed_messages_variable(
    stream: BinaryIO,
    *,
    as_hex: bool = False,
    client_id : str, 
    file_id: str, 
    db: Database,
    encoding: str,
    # encoding: str = "cp500",
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
        # print(f"mti_bytes: {mti_bytes} and bitmap_16: {bitmap_16}")
        mti, enc = detect_mti(
            payload=mti_bytes,
            encoding=encoding
        )  # robusto para EBCDIC digits
        # TODO: EXTRRAER ENCODING
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
                    # stream cortado
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

    # print(rows)
    return rows