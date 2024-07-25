import os
from enum import StrEnum, auto

import dotenv
import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database


log = Logger(__name__)


class _Layer(StrEnum):
    """
    Enum of file storage layers available.
    """

    LANDING = auto()
    STAGING = auto()
    OPERATIONAL = auto()
    ANALYTICS = auto()


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

    def read_binary(self) -> None:
        raise NotImplementedError

    def write_binary(self) -> None:
        raise NotImplementedError

    def read_parquet(self) -> None:
        raise NotImplementedError

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
