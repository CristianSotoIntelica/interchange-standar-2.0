from abc import ABC, abstractmethod
from typing import Type

import numpy as np
import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage


log = Logger(__name__)
fs = FileStorage()


class CalculatedField(ABC):
    """
    Abstract base class that all calculated field objects must inherit from.
    """

    @abstractmethod
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        """
        This method will always get called to calculate a new field.
        """
        pass


class ardef_country(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class authorization_code_valid(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                conditions = [
                    (source["authorization_code"].str[-1] == "x"),
                    (
                        source["authorization_code"]
                        .str[-5:]
                        .isin([" ", "0000", "00000", "0000n", "0000p", "0000y"])
                    ),
                ]
                condition_values = ["INVALID", "INVALID"]
                return pd.Series(
                    np.select(conditions, condition_values, default="VALID")
                )
            case _:
                raise NotImplementedError


class b2b_program_id(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class business_mode(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class business_transaction_type(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                conditions = [
                    (
                        source["draft_code"].isin(["05", "15", "25", "35"])
                        & ~source["merchant_category_code"].isin([4829, 6051, 7995])
                    ),
                    (
                        source["draft_code"].isin(["05", "15", "25", "35"])
                        & source["merchant_category_code"].isin([4829, 6051, 7995])
                    ),
                    (
                        source["draft_code"].isin(["06", "16", "26", "36"])
                        & (source["usage_code"] == 1)
                    ),
                    (
                        source["draft_code"].isin(["06", "16", "26", "36"])
                        & (source["usage_code"] == 1)
                        & source[
                            "special_condition_indicator_merchant_draft_indicator"
                        ].isin(["7", "8"])
                    ),
                    (
                        source["draft_code"].isin(["06", "16", "26", "36"])
                        & (source["usage_code"] == 1)
                        & (source["draft_code_qualifier_0"] == 2)
                    ),
                    (
                        source["draft_code"].isin(["07", "17", "27", "37"])
                        & (source["merchant_category_code"] == 6010)
                    ),
                    (
                        source["draft_code"].isin(["07", "17", "27", "37"])
                        & (source["merchant_category_code"] == 6011)
                    ),
                ]
                condition_values = [1, 3, 19, 20, 25, 21, 22]
                return pd.Series(np.select(conditions, condition_values, default=255))
            case _:
                raise NotImplementedError


class fast_funds(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class funding_source(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class issuer_country(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class jurisdiction(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class jurisdiction_assigned(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class jurisdiction_country(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class jurisdiction_region(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class nnss_indicator(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class product_id(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class product_subtype(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class reversal_indicator(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                conditions = [
                    (source["draft_code"].isin(["25", "26", "27", "35", "36", "37"]))
                ]
                condition_values = [1]
                return pd.Series(np.select(conditions, condition_values, default=0))
            case _:
                raise NotImplementedError


class technology_indicator(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


class timeliness(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return (
                    source["central_processing_date"] - source["purchase_date"]
                ).dt.days
            case _:
                raise NotImplementedError


class travel_indicator(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        raise NotImplementedError


def calculate_baseii_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="300-BASEII_CLN_DRAFTS",
    target_subdir="400-BASEII_CAL_DRAFTS",
) -> None:
    """
    Calculate additional fields from clean BASE II transaction data.
    """
    log.logger.info(
        f"Reading clean BASE II Transactions from {client_id} file {file_id}"
    )
    data = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=origin_subdir,
    )
    log.logger.info(f"Calculating additional fields from {client_id} file {file_id}")
    BASEII_FIELDS: list[Type[CalculatedField]] = [
        # ardef_country,
        authorization_code_valid,
        # b2b_program_id,
        # business_mode,
        business_transaction_type,
        # fast_funds,
        # funding_source,
        # issuer_country,
        # jurisdiction,
        # jurisdiction_assigned,
        # jurisdiction_country,
        # jurisdiction_region,
        # nnss_indicator,
        # product_id,
        # product_subtype,
        reversal_indicator,
        # technology_indicator,
        timeliness,
        # travel_indicator,
    ]
    fields = []
    for field in BASEII_FIELDS:
        calculated_field = field().calculate(data, type_record="draft")
        calculated_field.name = field.__name__
        fields.append(calculated_field)
    calculated_df = pd.concat(fields, axis=1)
    log.logger.info(
        f"Saving Visa Draft calculated fields from {client_id} file {file_id}"
    )
    fs.write_parquet(
        calculated_df, target_layer, client_id, file_id, subdir=target_subdir
    )


def calculate_sms_fields() -> None:
    raise NotImplementedError


def calculate_vss_fields() -> None:
    raise NotImplementedError


# calculate_baseii_fields(
#     fs.Layer.STAGING, fs.Layer.STAGING, "DEMO", "0A34F405E89F868E71010AD019D466AC"
# )
