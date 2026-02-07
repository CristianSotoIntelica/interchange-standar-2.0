import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage

fs = FileStorage()

def calculate_1240_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "400_IPM_1240_CLN",
        target_sub_dir: str = "500_IPM_1240_CAL",
) -> None:
    list_filepaths = fs.get_list_files_folderpath(
        layer=origin_layer,
        client_id=client_id,
        file_id=file_id,
        subdir=origin_sub_dir
    )

    for filepath in list_filepaths:
        df = fs.read_parquet_by_filepath(
            client_id=client_id,
            file_id=file_id,
            filepath=filepath
        )
        pass
    

def calculate_1442_fields(
        origin_layer: FileStorage.Layer,
        target_layer: FileStorage.Layer,
        client_id: str,
        file_id: str,
        origin_sub_dir: str = "400_IPM_1442_CLN",
        target_sub_dir: str = "500_IPM_1442_CAL",
) -> None:
    pass