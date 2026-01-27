import os
from enum import StrEnum, auto

import re
import dotenv
import pandas as pd
from typing import BinaryIO, List, Optional
import io

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from pathlib import Path

log = Logger(__name__)


class _Layer(StrEnum):
    """
    Enum of file storage layers available.
    """

    LANDING = auto()
    STAGING = auto()
    OPERATIONAL = auto()


class FileStorage:
    """
    Class to handle all file I/O operations.
    """

    Layer = _Layer

    def __init__(self) -> None:
        dotenv.load_dotenv()
        self.basepath = os.environ["ITX_DATALAKE_PATH"]

    def _get_file_path(
            self, layer: Layer, client_id: str, file_id: str, subdir: str = ""
    ) -> str:
        """
        Get the full path to a file based on its Client ID and File ID.
        """
        db = Database()
        file_details = db.read_records(
            table_name="file_control",
            fields=[
                "brand_id",
                "file_type",
                "file_processing_date",
                "landing_file_name",
            ],
            where={
                "client_id": client_id,
                "file_id": file_id,
            },
        ).iloc[0]

        if layer == self.Layer.LANDING:
            filepath = os.path.join(
                self.basepath,
                layer,
                client_id,
                file_details.loc["landing_file_name"],
            )
            return filepath

        filepath = os.path.join(
            self.basepath,
            layer,
            client_id,
            file_details.loc["brand_id"],
            file_details.loc["file_type"],
            file_details.loc["file_processing_date"],
            subdir,
            file_id,
        )
        return filepath
    
    def _get_folder_path(
            self, layer: Layer, client_id: str, file_id: str, subdir: str = ""
    ) -> str:
        db = Database()
        file_details = db.read_records(
            table_name="file_control", 
            fields=[
                "brand_id", "file_type", "file_processing_date", "landing_file_name"
            ],
            where={
                "client_id": client_id,
                "file_id": file_id
            }
        ).iloc[0]

        if layer == self.Layer.LANDING:
            filepath = os.path.join(
                self.basepath, layer, client_id, file_details.loc["landing_file_name"])
            return filepath
        
        filepath = os.path.join(
            self.basepath, layer, client_id, file_details.loc["brand_id"], 
            file_details.loc["file_type"], file_details.loc["file_processing_date"],
            subdir)
        return filepath

    def read_plaintext(
        self,
        layer: Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
        encoding: str = "Latin-1",
    ) -> pd.DataFrame:
        """
        Reads all non-empty lines of a plaintext file and returns a line dataframe.
        """
        try:
            log.logger.debug(f"Searching for {client_id} file {file_id}")
            filepath = self._get_file_path(layer, client_id, file_id, subdir)
            with open(filepath, mode="r", encoding=encoding) as file:
                log.logger.debug(f"Opening {client_id} file {file_id}")
                df = pd.DataFrame(file.read().split("\n"), columns=["lines"], dtype=str)
                return df[df["lines"] != ""]
        except OSError as e:
            log.logger.error(f"Error opening {client_id} file {file_id}: '{e}'")
            return pd.DataFrame([], columns=["lines"], dtype=str)

    def write_plaintext(self) -> None:
        raise NotImplementedError

    def read_binary(
        self, layer: Layer, client_id: str, file_id: str, subdir: str = "", 
        in_memory: bool = True) -> BinaryIO:
        try:
            log.logger.debug(f"Searching for {client_id} file {file_id}")
            filepath = self._get_file_path(layer, client_id, file_id, subdir)
            f = open(filepath, "rb")

            if not in_memory:
                return f #Debe cerrarse por la funcion que llamó el read_binary
            
            data  = f.read()
            f.close()
            stream_file = io.BytesIO(data)
            return stream_file

        except OSError as e:
            log.logger.error(f"Error opening {client_id} file {file_id}: '{e}'")
            raise

    def write_binary(self) -> None:
        raise NotImplementedError

    def read_parquet(
        self, layer: Layer, client_id: str, file_id: str, subdir: str = ""
    ) -> pd.DataFrame:
        """
        Read the given parquet file into a dataframe.
        """
        filepath = f"{self._get_file_path(layer, client_id, file_id, subdir)}.parquet"
        return pd.read_parquet(filepath)

    def write_parquet(
        self,
        data: pd.DataFrame,
        layer: Layer,
        client_id: str,
        file_id: str,
        subdir: str = "",
    ) -> None:
        """
        Write the given dataframe to a parquet file. Overwrites file if exists.
        """
        log.logger.debug(f"Writing {client_id} file {file_id} to parquet")
        filepath = f"{self._get_file_path(layer, client_id, file_id, subdir)}.parquet"
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        data.to_parquet(filepath, index=True)

    def write_parquet_by_filepath(
            self, data: pd.DataFrame, filepath: str, index: bool = False ) -> None:
        p = Path(filepath)
        os.makedirs(p.parent, exist_ok=True)
        data.to_parquet(p, index=index)

    def read_parquet_by_filepath(
            self, client_id: str, file_id: str, filepath: str) -> pd.DataFrame:

        log.logger.debug(f"Searching for {client_id} file {file_id}")
        return pd.read_parquet(filepath)
    
    def get_list_files_folderpath(
            self, layer: Layer, client_id: str, file_id: str, subdir: str = ""):
        
        "Return a list of filepaths of parquets derivaded from the same file_id"
        
        folder = Path(self._get_folder_path(
            layer=layer, client_id=client_id, file_id=file_id, subdir=subdir))
        
        if not folder.exists():
            return []
        
        prefix = f"{file_id}"
        files_lists = folder.rglob("*.parquet")
        results: List[Path] = []

        for file in files_lists:
            name = file.name
            # Debe empezar con {file_id}
            if not name.startswith(prefix):
                continue

            results.append(file)

        # Ordenar por nombres
        results.sort(key=lambda x: x.name)

        # Devolver el resultado
        return [str(file) for file in results]

    def build_target_name_from_raw_filepath(self, raw_filepath: str) -> str:
        stem = Path(raw_filepath).stem

        m = re.match(r"^(?P<md5>[0-9a-fA-F]{32})_(?P<block>\d+)_(?P<mti>\d{4})$", stem)
        if m:
            return f"{m.group('md5')}_{m.group('block')}_{m.group('mti')}.parquet"

        m = re.match(r"^(?P<md5>[0-9a-fA-F]{32})_part-\d+_(?P<block>\d+)_(?P<mti>\d{4})$", stem)
        if m:
            return f"{m.group('md5')}_{m.group('block')}_{m.group('mti')}.parquet"
        
        raise ValueError(f"No reconozco nomenclatura del parquet raw: {Path(raw_filepath).name}")
    

    def build_target_parquet_filepath_from_raw(
            self, raw_filepath: str, target_layer: Layer, client_id: str, file_id: str,
            target_subdir: str,) -> str:
        target_folder = Path(self._get_folder_path(
            layer=target_layer, client_id=client_id, file_id=file_id, 
            subdir=target_subdir))
        target_filename = self.build_target_name_from_raw_filepath(raw_filepath)
        return str(target_folder / target_filename)
