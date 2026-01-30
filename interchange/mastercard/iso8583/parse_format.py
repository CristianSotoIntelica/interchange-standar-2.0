from __future__ import annotations
from typing import Dict, Optional, Any
from collections.abc import Set as AbstractSet



import pandas as pd

from interchange.mastercard.iso8583.decode_digits import decode_digits

from typing import Iterable
from typing import Callable


from interchange.mastercard.iso8583.split_mti import bitmap_bits
from interchange.mastercard.iso8583.decode_digits import decode_digits

DEFAULT_NUMERIC_DES = frozenset({
    2,3,4,5,6,9,10,12,14,23,24,25,26,30,37,38,49,50,51,71,73,93,94,95,100
})

DEFAULT_BINARY_DES = frozenset({55})
DEFAULT_EBCDIC_TEXT_DES = frozenset({43, 48,22})  # 43 seguro; 22 según lo que viste (c3/c4)

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
        "file_idn": None,
        "file_dt": None,
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

        if de in bin_: #DE 55
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
            else:
                base[col] = decode_text_best(raw, enc)
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

def add_headers_fields_697(df: pd.DataFrame) -> None:
    """
    Para headers (function_code == '697'):
      file_idn = SUBSTRING(de_48, 8, CAST(SUBSTRING(de_48,5,3) AS INT))
      file_dt = SUBSTRING(File_ID, 4, 6)

    Modifica el DataFrame IN-PLACE.
    Asume que File_ID y File_DT YA existen.
    """

    mask = df["function_code"].astype(str).eq("697")
    if not mask.any():
        return

    s = df.loc[mask, "de_48"].astype("string")

    # largo = SUBSTRING(de_48,5,3)
    largo = pd.to_numeric(s.str.slice(4, 7), errors="coerce").fillna(0).astype(int)

    # resto desde posición 8
    resto = s.str.slice(7)

    # file_id con largo variable (pandas no soporta slice variable)
    file_id = [
        r[:l] if pd.notna(r) and l > 0 else pd.NA
        for r, l in zip(resto, largo)
    ]

    file_id = pd.Series(file_id, index=s.index, dtype="string")

    df.loc[mask, "file_idn"] = file_id
    df.loc[mask, "file_dt"] = file_id.str.slice(3, 9)


def apply_block_file_context_697(
    df: pd.DataFrame,
    *,
    state: dict[int, tuple[str, str]],
    strict: bool = False,
) -> None:
    """
    Aplica el contexto file_idn/file_dt a todas las filas del DataFrame
    según el header 697 y el block.

    - df: DataFrame wide del chunk
    - state: dict persistente entre chunks {block: (file_idn, file_dt)}
    - strict: si True, falla si detecta más de un 697 por block

    Modifica df IN-PLACE.
    """

    # -------------------------------------------------
    # 1) Extraer headers 697 del chunk
    # -------------------------------------------------
    hdr = df["function_code"].astype("string").str.strip().eq("697")
    if hdr.any():
        h = (
            df.loc[hdr, ["block", "file_idn", "file_dt"]]
            .dropna(subset=["block", "file_idn"])
        )

        if not h.empty:
            h["block"] = h["block"].astype(int)

            # Validación: más de un header por block
            dup = h["block"][h["block"].duplicated(keep=False)]
            if not dup.empty:
                blocks = sorted(dup.unique().tolist())
                msg = f"Más de un header 697 para block(s): {blocks}"
                if strict:
                    raise ValueError(msg)
                else:
                    # loggear si quieres, aquí no pisamos nada
                    print(f"[WARN] {msg}")

            # Solo agregamos blocks nuevos (no pisamos)
            new = ~h["block"].isin(state.keys())
            h_new = h.loc[new]

            if not h_new.empty:
                state.update(
                    dict(
                        zip(
                            h_new["block"].to_list(),
                            zip(
                                h_new["file_idn"].astype(str).to_list(),
                                h_new["file_dt"].astype(str).to_list(),
                            ),
                        )
                    )
                )

    # -------------------------------------------------
    # 2) Asignar contexto por block (map vectorizado)
    # -------------------------------------------------
    m = df["block"].notna()
    if m.any() and state:
        pre = df.loc[m, "block"].astype(int).map(state)  # tuple o NaN
        ok = pre.notna()
        if ok.any():
            idx = pre.index[ok]
            vals = pre.loc[idx].tolist()

            df.loc[idx, "file_idn"] = [v[0] for v in vals]
            df.loc[idx, "file_dt"] = [v[1] for v in vals]
