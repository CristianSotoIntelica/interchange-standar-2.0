
from pathlib import Path

def extract_fc_from_filepath(filepath: str | Path) -> str:
    name = Path(filepath).name
    return name.rsplit("_", 1)[-1].replace(".parquet", "")