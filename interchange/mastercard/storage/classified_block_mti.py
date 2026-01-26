import pandas as pd 
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Dict, Tuple

from interchange.persistence.file import FileStorage
fs = FileStorage()


def _canonical_schema_from_de_spec(de_spec: dict) -> pa.Schema:
    # columnas base fijas
    fields = [
        pa.field("msg_no", pa.int64()),
        pa.field("block", pa.int64()),
        pa.field("mti", pa.string()),
        pa.field("enc", pa.string()),
        pa.field("function_code", pa.string()),
        pa.field("function_role", pa.string()),
        pa.field("parse_ok", pa.bool_()),
    ]
    # todos los DE como string
    for de in sorted(de_spec.keys()):
        fields.append(pa.field(f"de_{de}", pa.string()))
    return pa.schema(fields)

def _ensure_and_cast(table: pa.Table, schema: pa.Schema) -> pa.Table:
    # agrega columnas faltantes
    missing = [n for n in schema.names if n not in table.schema.names]
    if missing:
        nrows = table.num_rows
        for col in missing:
            table = table.append_column(col, pa.nulls(nrows, type=schema.field(col).type))

    # reordenar y castear
    table = table.select(schema.names)
    return table.cast(schema, safe=False)

# ELIMINAR
# def classified_block_mti_parts(
#         df, target_layer:FileStorage.Layer , client_id:str, file_id:str,out_dir, *, 
#         part_id: int):
#     """
#     Escribe parquets por (block, mti) en archivos part-XXXX.parquet
#     para soportar chunks SIN sobrescribir.
#     """
#     out_dir = Path(out_dir)

#     for (block, mti), g in df.groupby(["block", "mti"], sort=False):
#         g: pd.DataFrame
#         block_str = "NA" if block != block else str(int(block))  # NaN-safe
#         mti_str = "UNK" if mti is None else str(mti)
#         name_block = f"part-{part_id:06d}_{block_str}_{mti_str}"

#         if mti_str == '1240':
#             fs.write_parquet_per_block(
#                 data=g, layer=target_layer, client_id=client_id, file_id=file_id, 
#                 subdir="100_IPM_1240_RAW", name_block=name_block)
#         elif mti_str == '1442':
#             fs.write_parquet_per_block(
#                 data=g, layer=target_layer, client_id=client_id, file_id=file_id, 
#                 subdir="100_IPM_1442_RAW", name_block=name_block)
#         elif mti_str == '1644':
#             fs.write_parquet_per_block(
#                 data=g, layer=target_layer, client_id=client_id, file_id=file_id, 
#                 subdir="100_IPM_1644_RAW", name_block=name_block)
#         elif mti_str == '1740':
#             fs.write_parquet_per_block(
#                 data=g, layer=target_layer, client_id=client_id, file_id=file_id, 
#                 subdir="100_IPM_1740_RAW", name_block=name_block)

def subdir_for_mti(mti: str) -> str:
    mti = str(mti)
    if mti == "1240":
        return "100_IPM_1240_RAW"
    elif mti == "1442":
        return "100_IPM_1442_RAW"
    elif mti == "1644":
        return "100_IPM_1644_RAW"
    elif mti == "1740":
        return "100_IPM_1740_RAW"
    return "100_IPM_UNK_RAW"

def _base_dir_for_subdir(fs, layer, client_id: str, file_id: str, subdir: str) -> Path:
    base = Path(fs._get_file_path(layer, client_id, file_id, subdir=subdir))
    return base.parent  

def write_parquet_by_mti_block_streaming(
    df_chunk: pd.DataFrame,
    *,
    fs,
    target_layer,
    client_id: str,
    file_id: str,
    schema: pa.Schema,
    writers: dict,
) -> None:

    df_chunk = df_chunk[df_chunk["block"].notna()]
    if df_chunk.empty:
        return

    for (block, mti), g in df_chunk.groupby(["block", "mti"], sort=False):
        block_i = int(block)
        mti_s = str(mti)

        subdir = subdir_for_mti(mti_s)

        base_dir = _base_dir_for_subdir(
            fs, target_layer, client_id, file_id, subdir
        )

        out_dir = base_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{file_id}_{block_i}_{mti_s}.parquet"
        out_path = out_dir / filename

        key = (file_id, block_i, mti_s)

        table = pa.Table.from_pandas(g, preserve_index=False)
        table = _ensure_and_cast(table, schema)

        # writer único por archivo
        if key not in writers:
            writers[key] = pq.ParquetWriter(
                out_path.as_posix(),
                schema,
                compression="snappy",
                use_dictionary=True,
            )

        writers[key].write_table(table)


def finalize_writers(writers: Dict[Tuple[str, int, str], pq.ParquetWriter]) -> None:
    for w in writers.values():
        w.close()
    writers.clear()