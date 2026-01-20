import pandas as pd 
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq



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
            table = table.append_column(col, pa.nulls(nrows))

    # reordenar y castear
    table = table.select(schema.names)
    return table.cast(schema, safe=False)

def classified_block_mti(df, test_path):
    sub_dfs = {}
 
    for (c1, c2), group in df.groupby(['block', 'mti']):
        key = f"name_{c1}_{c2}"
        sub_dfs[key] = group.reset_index(drop=True)
    for key, subdf in sub_dfs.items():
    
        col2_value = key.split('_')[-1]   # segundo parámetro real
        suffix = col2_value[-4:]          # RIGHT(4)
    
        if suffix == '1240':
            filepath = test_path / f"{key}.parquet" 
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)
        
        elif suffix == '1442':
            filepath = test_path / f"{key}.parquet"
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)
        
        elif suffix == '1644':
            filepath = test_path / f"{key}.parquet"
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)
        
        elif suffix == '1740':
            filepath = test_path / f"{key}.parquet"
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            subdf.to_parquet(filepath, index=True)


def classified_block_mti_parts(df, out_dir, *, part_id: int):
    """
    Escribe parquets por (block, mti) en archivos part-XXXX.parquet
    para soportar chunks SIN sobrescribir.
    """
    out_dir = Path(out_dir)

    for (block, mti), g in df.groupby(["block", "mti"], sort=False):
        block_str = "NA" if block != block else str(int(block))  # NaN-safe
        mti_str = "UNK" if mti is None else str(mti)

        folder = out_dir / f"block={block_str}" / f"mti={mti_str}"
        folder.mkdir(parents=True, exist_ok=True)

        g.to_parquet(folder / f"part-{part_id:06d}.parquet", index=False)

def compact_parquet_parts(root_dir, *, de_spec: dict):
    """
    Une part-*.parquet -> final.parquet por cada (block,mti),
    usando schema canónico estable (sin mismatch).
    """
    root_dir = Path(root_dir)
    schema = _canonical_schema_from_de_spec(de_spec)

    for mti_dir in root_dir.glob("block=*/mti=*"):
        part_files = sorted(mti_dir.glob("part-*.parquet"))
        if not part_files:
            continue

        final_path = mti_dir / "final.parquet"
        writer = pq.ParquetWriter(final_path, schema, compression="snappy")

        try:
            for p in part_files:
                pf = pq.ParquetFile(p)
                for batch in pf.iter_batches():
                    table = pa.Table.from_batches([batch])
                    table = _ensure_and_cast(table, schema)
                    writer.write_table(table)
        finally:
            writer.close()

        # borra parts luego de compactar
        for p in part_files:
            p.unlink()