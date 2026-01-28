from __future__ import annotations
from typing import Dict, Optional, Set, Any
import binascii

import pandas as pd

from interchange.mastercard.iso8583.decode_digits import decode_digits

from typing import Iterable
from typing import Callable

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

    
    if not parse_ok or not isinstance(body_hex, str) or not isinstance(bitmap_hex, str) or not enc:
        # deja columnas DE_... como None igual (para consistencia)
        for de in sorted(de_spec.keys()):
            base[f"de_{de}"] = None
        return base

    body = bytes.fromhex(body_hex)
    bitmap = bytes.fromhex(bitmap_hex)

    # IMPORTANTE: bitmap_bits está en split_mti.py, lo importamos aquí para que interpreter quede limpio
    from interchange.mastercard.iso8583.split_mti import bitmap_bits
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

    from interchange.mastercard.iso8583.split_mti import bitmap_bits
    fields = bitmap_bits(bitmap)

    # Splitear los DE en formato HEX
    raw_map = parse_des_one_pass(body=body, fields=fields, enc=enc, de_spec=de_spec, max_de=24) 
    
    raw24 = raw_map.get(24)
    if raw24 is None:
        return None

    return decode_digits(raw24, enc)


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
