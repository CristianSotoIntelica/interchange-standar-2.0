from typing import BinaryIO

def unblock_1014(
    stream_file: BinaryIO, payload_size: int = 1012, sep_size: int = 2, 
    valid_seps: tuple[bytes, ...] = (b"", b"\x20\x20", b"\x40\x40")) -> bytes:
    
    stream_file.seek(0)
    out_bytes = bytearray()

    while True:
        chunk = stream_file.read(payload_size)
        if chunk:
            out_bytes.extend(chunk)
        if len(chunk) < payload_size:
            break

        sep = stream_file.read(sep_size)

        if sep not in valid_seps:
            stream_file.seek(stream_file.tell() - len(sep))
        else:
            print(sep)

    return bytes(out_bytes)