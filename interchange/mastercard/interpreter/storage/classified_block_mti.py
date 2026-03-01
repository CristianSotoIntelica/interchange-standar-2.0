import pandas as pd 
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, Tuple

from interchange.persistence.file import FileStorage
fs = FileStorage()


def _canonical_schema_from_de_spec(de_spec: dict) -> pa.Schema:
    # columnas base fijas
    fields = [
        pa.field("file_idn", pa.string()),
        pa.field("file_dt", pa.string()),
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
    """
    Alinea la tabla al schema canonical: agrega columnas faltantes como nulls,
    reordena y castea tipos.

    POR QUÉ SE REESCRIBIÓ:
      El código anterior usaba un loop de append_column que creaba una tabla
      PyArrow nueva en cada iteración (~95 veces por bloque). PyArrow tables
      son inmutables, así que cada append_column = copia completa de la tabla
      anterior + columna nueva. Con cientos de bloques por archivo, esto
      acumulaba GBs en el memory pool interno de PyArrow.

      Ahora construimos todos los arrays en una lista primero (sin tablas
      intermedias) y hacemos una única llamada pa.table() al final.
      El peak de RAM pasa de ~2x tamaño tabla × N bloques → ~1x tamaño tabla.
    """
    present = set(table.schema.names)
    nrows = table.num_rows

    # Una sola pasada sobre el schema:
    # - Si la columna existe: tomamos la referencia y casteamos si es necesario
    # - Si no existe: creamos un array de nulls del tipo correcto
    # Sin tablas intermedias, solo una lista de arrays.
    arrays = []
    for field in schema:
        if field.name in present:
            col = table.column(field.name)
            if col.type != field.type:
                col = col.cast(field.type, safe=False)
            arrays.append(col)
        else:
            arrays.append(pa.nulls(nrows, type=field.type))

    # Una única construcción de tabla al final con el schema exacto
    return pa.table(arrays, schema=schema)


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

    df_chunk = df_chunk[df_chunk["file_idn"].notna()]
    if df_chunk.empty:
        return

    for (file_idn, mti), g in df_chunk.groupby(["file_idn", "mti"], sort=False):
        file_idn = str(file_idn)
        mti_s = str(mti)

        subdir = subdir_for_mti(mti_s)

        base_dir = _base_dir_for_subdir(
            fs, target_layer, client_id, file_id, subdir
        )

        out_dir = base_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{file_id}_{file_idn}_{mti_s}.parquet"
        out_path = out_dir / filename

        key = (file_id, file_idn, mti_s)

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



def write_parquet_by_mti_block_streaming2(
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