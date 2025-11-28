from abc import ABC, abstractmethod
from datetime import date
from typing import Type

import numpy as np
import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage


pd.set_option("future.no_silent_downcasting", True)

log = Logger(__name__)
fs = FileStorage()


class CalculatedField(ABC):
    """
    Abstract base class that all calculated field objects must inherit from.
    """

    def __init__(
        self, client_data: pd.Series, file_data: pd.Series, ardef_data: pd.DataFrame
    ) -> None:
        super().__init__()
        self.client = client_data
        self.file = file_data
        self.ardef = ardef_data

    def _get_from_ardef(self, intervals: pd.Series, ardef_field: str) -> pd.Series:
        """
        Get an ARDEF field's values corresponding to a series of account intervals.
        """
        df = intervals.to_frame()
        df = pd.merge(
            df,
            self.ardef[["account_interval", ardef_field]],
            on="account_interval",
            how="left",
        )
        return df[ardef_field]

    @abstractmethod
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        """
        This method will always get called to calculate a new field.
        """
        pass


class acquirer_bin(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "sms":
                return source["retrieval_reference_number"].str.slice(0, 6)
            case _:
                raise NotImplementedError


class ardef_country(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(source["account_interval"], "ardef_country")
            case "sms":
                return self._get_from_ardef(source["account_interval"], "ardef_country")
            case _:
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
            case "sms":
                conditions = [
                    (source["authorization_id_resp._code"].str[-1] == "x"),
                    (
                        source["authorization_id_resp._code"]
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
        match type_record:
            case "draft":
                return self._get_from_ardef(
                    source["account_interval"], "b2b_program_id"
                )
            case "sms":
                return self._get_from_ardef(
                    source["account_interval"], "b2b_program_id"
                )
            case _:
                raise NotImplementedError


class business_application_id(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                df = source[
                    [
                        "business_application_id_fl",
                        "business_application_id_cr",
                        "business_application_id_ft",
                    ]
                ]
                df = df.apply(lambda x: x.str.strip())
                df = df.replace("", np.nan)
                return df.bfill(axis=1).iloc[:, 0]
            case _:
                raise NotImplementedError


class business_format_code(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                df = source[
                    [
                        "business_format_code_cr",
                        "business_format_code_fl",
                        "business_format_code_ft",
                        "business_format_code_df",
                        "business_format_code_pd",
                        "business_format_code_sd",
                        "business_format_code_sp",
                    ]
                ]
                df = df.apply(lambda x: x.str.strip())
                df = df.replace("", np.nan)
                return df.bfill(axis=1).iloc[:, 0]
            case _:
                raise NotImplementedError


class business_mode(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                conditions = [
                    (
                        source["draft_code"].isin(["05", "25", "06", "26", "07", "27"])
                        & (self.file["file_type"] == "OUT")
                    ),
                    (
                        source["draft_code"].isin(["15", "35", "16", "36", "17", "37"])
                        & (self.file["file_type"] == "OUT")
                    ),
                    (
                        source["draft_code"].isin(["05", "25", "06", "26", "07", "27"])
                        & (self.file["file_type"] == "IN")
                    ),
                    (
                        source["draft_code"].isin(["15", "35", "16", "36", "17", "37"])
                        & (self.file["file_type"] == "IN")
                    ),
                ]
                condition_values = ["A", "I", "I", "A"]  # Acquiring or Issuing
                return pd.Series(np.select(conditions, condition_values, default=""))
            case "sms":
                return source["issuer_acquirer_indicator"]
            case _:
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
            case "sms":
                rmt = source["request_message_type"]
                rc = source["response_code"]
                pc = source["processing_code"].str[:2]
                pos = source["pos_condition_code"]
                mcc = source["merchant's_type"]

                # Condiciones del primer bloque: response_code = '00'
                cond_success = rmt.isin(["0200", "0220", "0400", "0420"]) & (rc == "00")

                # Segundo bloque: response_code != '00'
                cond_decline = rmt.isin(["0200", "0220", "0400", "0420"]) & (rc != "00")

                conditions = [
                    (
                        cond_success
                        & (pc == "00")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "01")
                        & ~pos.isin(["13", "51"])
                        & (mcc == 6010)
                    ),
                    (
                        cond_success
                        & (pc == "01")
                        & ~pos.isin(["13", "51"])
                        & (mcc == 6011)
                    ),
                    (
                        cond_success
                        & (pc == "10")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "11")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "19")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "20")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "22")
                        & pos.isin(["13"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "26")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "29")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (
                        cond_success
                        & (pc == "30")
                        & ~pos.isin(["13", "51"])
                        & (mcc == 6011)
                    ),
                    (
                        cond_success
                        & (pc == "40")
                        & ~pos.isin(["13", "51"])
                        & (mcc == 6011)
                    ),
                    (
                        cond_success
                        & (pc == "50")
                        & ~pos.isin(["13", "51"])
                        & ~mcc.isin([4815, 6010, 6011])
                    ),
                    (cond_decline & (mcc != 6011)),
                    (cond_decline & (mcc == 6011)),
                ]

                condition_values = [
                    1,
                    21,
                    22,
                    30,
                    3,
                    115,
                    19,
                    20,
                    25,
                    200,
                    247,
                    250,
                    27,
                    236,
                    249,
                ]

                return pd.Series(
                    np.select(conditions, condition_values, default=np.nan),
                    index=source.index,
                )
            case _:
                raise NotImplementedError


class fast_funds(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(source["account_interval"], "fast_funds")
            case "sms":
                return self._get_from_ardef(source["account_interval"], "fast_funds")
            case _:
                raise NotImplementedError


class funding_source(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(
                    source["account_interval"], "account_funding_source"
                )
            case "sms":
                return self._get_from_ardef(
                    source["account_interval"], "account_funding_source"
                )
            case _:
                raise NotImplementedError


class issuer_bin_8(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return source["account_number"].str.replace("*", "0").str.slice(0, 8)
            case "sms":
                return source["card_number"].str.replace("*", "0").str.slice(0, 8)
            case _:
                raise NotImplementedError


class issuer_country(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(source["account_interval"], "country")
            case "sms":
                return self._get_from_ardef(source["account_interval"], "country")
            case _:
                raise NotImplementedError


class issuer_region(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(source["account_interval"], "region")
            case "sms":
                return self._get_from_ardef(source["account_interval"], "region")
            case _:
                raise NotImplementedError


class jurisdiction(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        db = Database()
        country = db.read_records(
            table_name="country",
            fields=["country_code", "visa_region_code"],
        )
        country.rename(
            columns={
                "country_code": "merchant_country_code",
                "visa_region_code": "merchant_region_code",
            },
            inplace=True,
        )
        ar_countries = self._get_from_ardef(source["account_interval"], "ardef_country")
        ar_countries.name = "ardef_country"
        ar_regions = self._get_from_ardef(source["account_interval"], "ardef_region")
        ar_regions.name = "ardef_region"
        issuing_bins_6 = str(self.client["issuing_bins_6_digits"]).split(",")
        issuing_bins_8 = str(self.client["issuing_bins_8_digits"]).split(",")
        acquiring_bins = str(self.client["acquiring_bins"]).split(",")
        match type_record:
            case "draft":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    on="merchant_country_code",
                )
                source = source.join(ar_countries, how="left")
                source = source.join(ar_regions, how="left")
                conditions = [
                    (
                        (source["merchant_country_code"] == source["ardef_country"])
                        & (self.file["file_type"] == "OUT")
                        & (
                            (source["collection_only_flag"] == "C")
                            | (
                                source["account_number"]
                                .str.replace("*", "0")
                                .str.slice(0, 6)
                                .isin(issuing_bins_6)
                            )
                            | (
                                source["account_number"]
                                .str.replace("*", "0")
                                .str.slice(0, 8)
                                .isin(issuing_bins_8)
                            )
                        )
                    ),
                    (
                        (source["merchant_country_code"] == source["ardef_country"])
                        & (self.file["file_type"] == "IN")
                        & (
                            (source["collection_only_flag"] == "C")
                            | (
                                source["account_reference_number_acquiring_identifier"]
                                .astype(str)
                                .str.zfill(6)
                                .isin(acquiring_bins)
                            )
                        )
                    ),
                    (source["merchant_country_code"] == source["ardef_country"]),
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] == source["ardef_region"])
                    ),
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] != source["ardef_region"])
                    ),
                ]
                condition_values = [
                    "on-us",
                    "on-us",
                    "off-us",
                    "intraregional",
                    "interregional",
                ]
                return pd.Series(np.select(conditions, condition_values, default=""))
            case "sms":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    left_on="card_acceptor_country",
                    right_on="merchant_country_code",
                )
                source = source.join(ar_countries, how="left")
                source = source.join(ar_regions, how="left")
                conditions = [
                    (
                        (source["merchant_country_code"] == source["ardef_country"])
                        & (source["issuer_acquirer_indicator"] == "A")
                        & (
                            (
                                source["card_number"]
                                .str.replace("*", "0")
                                .str.slice(0, 6)
                                .isin(issuing_bins_6)
                            )
                            | (
                                source["card_number"]
                                .str.replace("*", "0")
                                .str.slice(0, 8)
                                .isin(issuing_bins_8)
                            )
                        )
                    ),
                    (
                        (source["merchant_country_code"] == source["ardef_country"])
                        & (source["issuer_acquirer_indicator"] == "I")
                        & (
                            source["acquiring_institution_id_1"]
                            .astype(str)
                            .str.zfill(6)
                            .isin(acquiring_bins)
                        )
                    ),
                    (source["merchant_country_code"] == source["ardef_country"]),
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] == source["ardef_region"])
                    ),
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] != source["ardef_region"])
                    ),
                ]
                condition_values = [
                    "on-us",
                    "on-us",
                    "off-us",
                    "intraregional",
                    "interregional",
                ]
                return pd.Series(np.select(conditions, condition_values, default=""))
            case _:
                raise NotImplementedError


class jurisdiction_assigned(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        db = Database()
        country = db.read_records(
            table_name="country",
            fields=["country_code", "visa_region_code"],
        )
        country.rename(
            columns={
                "country_code": "merchant_country_code",
                "visa_region_code": "merchant_region_code",
            },
            inplace=True,
        )
        ar_countries = self._get_from_ardef(source["account_interval"], "ardef_country")
        ar_countries.name = "ardef_country"
        ar_regions = self._get_from_ardef(source["account_interval"], "ardef_region")
        ar_regions.name = "ardef_region"
        match type_record:
            case "draft":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    on="merchant_country_code",
                )
                source = source.join(ar_countries, how="left")
                source = source.join(ar_regions, how="left")
                source["jurisdiction_assigned"] = ""  # Initialize field
                source.loc[
                    (source["merchant_country_code"] == source["ardef_country"]),
                    "jurisdiction_assigned",
                ] = source["merchant_country_code"]
                source.loc[
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] == source["ardef_region"])
                    ),
                    "jurisdiction_assigned",
                ] = source["ardef_region"]
                source.loc[
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] != source["ardef_region"])
                    ),
                    "jurisdiction_assigned",
                ] = "9"  # Interregional
                return source["jurisdiction_assigned"]
            case "sms":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    left_on="card_acceptor_country",
                    right_on="merchant_country_code",
                )
                source = source.join(ar_countries, how="left")
                source = source.join(ar_regions, how="left")
                source["jurisdiction_assigned"] = ""  # Initialize field
                source.loc[
                    (source["merchant_country_code"] == source["ardef_country"]),
                    "jurisdiction_assigned",
                ] = source["merchant_country_code"]
                source.loc[
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] == source["ardef_region"])
                    ),
                    "jurisdiction_assigned",
                ] = source["ardef_region"]
                source.loc[
                    (
                        (source["merchant_country_code"] != source["ardef_country"])
                        & (source["merchant_region_code"] != source["ardef_region"])
                    ),
                    "jurisdiction_assigned",
                ] = "9"  # Interregional
                return source["jurisdiction_assigned"]
            case _:
                raise NotImplementedError


class jurisdiction_country(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return source["merchant_country_code"]
            case "sms":
                return source["card_acceptor_country"]
            case _:
                raise NotImplementedError


class jurisdiction_region(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        db = Database()
        country = db.read_records(
            table_name="country",
            fields=["country_code", "visa_region_code"],
        )
        country.rename(
            columns={
                "country_code": "merchant_country_code",
                "visa_region_code": "merchant_region_code",
            },
            inplace=True,
        )
        match type_record:
            case "draft":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    on="merchant_country_code",
                )
                return source["merchant_region_code"]
            case "sms":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    left_on="card_acceptor_country",
                    right_on="merchant_country_code",
                )
                return source["merchant_region_code"]
            case _:
                raise NotImplementedError


class message_reason_code(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                df = source[
                    [
                        "message_reason_code_df",
                        "message_reason_code_sd",
                        "message_reason_code_sp",
                    ]
                ]
                df = df.apply(lambda x: x.str.strip())
                df = df.replace("", np.nan)
                return df.bfill(axis=1).iloc[:, 0]
            case _:
                raise NotImplementedError


class network_identification_code(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                df = source[
                    [
                        "network_identification_code_df",
                        "network_identification_code_sd",
                        "network_identification_code_sp",
                    ]
                ]
                df = df.apply(lambda x: x.str.strip())
                df = df.replace("", np.nan)
                return df.bfill(axis=1).iloc[:, 0]
            case _:
                raise NotImplementedError


class nnss_indicator(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(
                    source["account_interval"], "nnss_indicator"
                )
            case "sms":
                return self._get_from_ardef(
                    source["account_interval"], "nnss_indicator"
                )
            case _:
                raise NotImplementedError


class processing_code_transaction_type(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "sms":
                return source["processing_code"].str.slice(0, 2)
            case _:
                raise NotImplementedError


class product_id(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(source["account_interval"], "product_id")
            case "sms":
                return self._get_from_ardef(source["account_interval"], "product_id")
            case _:
                raise NotImplementedError


class product_subtype(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(
                    source["account_interval"], "product_subtype"
                )
            case "sms":
                return self._get_from_ardef(
                    source["account_interval"], "product_subtype"
                )
            case _:
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
            case "sms":
                conditions = [
                    (
                        source["request_message_type"].isin(["0200", "0220"])
                        & source["response_code"].isin(["00"])
                    ),
                    (
                        source["request_message_type"].isin(["0400", "0420"])
                        & source["response_code"].isin(["00"])
                    ),
                ]
                condition_values = [0, 1]
                return pd.Series(np.select(conditions, condition_values, default=0))
            case _:
                raise NotImplementedError


class source_amount(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "sms":
                return source["draft_amount"]
            case _:
                raise NotImplementedError


class source_currency_code_alphabetic(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        db = Database()
        country = db.read_records(
            table_name="currency",
            fields=["currency_numeric_code", "currency_alphabetic_code"],
        )
        country.rename(
            columns={
                "currency_numeric_code": "source_currency_code",
                "currency_alphabetic_code": "source_currency_code_alphabetic",
            },
            inplace=True,
        )
        match type_record:
            case "draft":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    on="source_currency_code",
                )
                return source["source_currency_code_alphabetic"]
            case "sms":
                source = pd.merge(
                    source,
                    country,
                    how="left",
                    left_on="draft_currency_code",
                    right_on="source_currency_code",
                )
                return source["source_currency_code_alphabetic"]
            case _:
                raise NotImplementedError


class surcharge_amount(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                df = source[
                    [
                        "surcharge_amount_df",
                        "surcharge_amount_sd",
                        "surcharge_amount_sp",
                    ]
                ]
                return df.max(axis=1)
            case _:
                raise NotImplementedError


class technology_indicator(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(
                    source["account_interval"], "technology_indicator"
                )
            case "sms":
                return self._get_from_ardef(
                    source["account_interval"], "technology_indicator"
                )
            case _:
                raise NotImplementedError


class timeliness(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return (
                    source["central_processing_date"] - source["purchase_date"]
                ).dt.days
            case "sms":
                return (
                    source["settlement_date_sms"] - source["local_draft_date"]
                ).dt.days
            case _:
                raise NotImplementedError


class transaction_code_sms(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        db = Database()
        transaction_type = db.read_records(
            table_name="visa_transaction_type",
            fields=["business_transaction_type_id", "transaction_type_id"],
        )

        btt_series = business_transaction_type(
            self.client,
            self.file,
            self.ardef,
        ).calculate(source, type_record)
        btt_series.name = "business_transaction_type_id"

        reversal_series = reversal_indicator(
            self.client,
            self.file,
            self.ardef,
        ).calculate(source, type_record)
        reversal_series.name = "reversal_indicator"

        df = pd.DataFrame(
            {
                "business_transaction_type_id": btt_series,
                "reversal_indicator": reversal_series,
            }
        )
        df["business_transaction_type_id"] = (
            df["business_transaction_type_id"]
            .astype(float)  # asegura que todos sean números
            .astype("Int64")  # Pandas Int64 acepta NaN
            .astype(str)  # finalmente a string para merge con transaction_type
        )
        df = df.merge(
            transaction_type[["business_transaction_type_id", "transaction_type_id"]],
            on="business_transaction_type_id",
            how="left",
        )
        match type_record:
            case "sms":
                conditions = [
                    (df["transaction_type_id"] == "PUR")
                    & (df["reversal_indicator"] == 0),
                    (df["transaction_type_id"] == "CRD")
                    & (df["reversal_indicator"] == 0),
                    (df["transaction_type_id"] == "CSH")
                    & (df["reversal_indicator"] == 0),
                    (df["transaction_type_id"] == "PUR")
                    & (df["reversal_indicator"] == 1),
                    (df["transaction_type_id"] == "CRD")
                    & (df["reversal_indicator"] == 1),
                    (df["transaction_type_id"] == "CSH")
                    & (df["reversal_indicator"] == 1),
                ]

                condition_values = ["05", "06", "07", "25", "26", "27"]
                return pd.Series(
                    np.select(conditions, condition_values, default=""),
                    index=source.index,
                )
            case _:
                raise NotImplementedError


class travel_indicator(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                return self._get_from_ardef(
                    source["account_interval"], "travel_indicator"
                )
            case "sms":
                return self._get_from_ardef(
                    source["account_interval"], "travel_indicator"
                )
            case _:
                raise NotImplementedError


class type_of_purchase(CalculatedField):
    def calculate(self, source: pd.DataFrame, type_record: str) -> pd.Series:
        match type_record:
            case "draft":
                df = source[
                    [
                        "type_of_purchase_fl",
                        "type_of_purchase_ft",
                    ]
                ]
                df = df.apply(lambda x: x.str.strip())
                df = df.replace("", np.nan)
                return df.bfill(axis=1).iloc[:, 0]
            case _:
                raise NotImplementedError


def _get_client_data(client_id: str) -> pd.Series:
    """
    Get key metadata associated to a client.
    """
    db = Database()
    fd = db.read_records(
        table_name="client",
        fields=[
            "local_currency_code",
            "settlement_currency_code",
            "report_currency_code",
            "issuing_bins_6_digits",
            "issuing_bins_8_digits",
            "acquiring_bins",
            "customer_country",
        ],
        where={"client_id": client_id},
    )
    return fd.iloc[0]


def _get_file_data(client_id: str, file_id: str) -> pd.Series:
    """
    Get key metadata associated to an interchange file.
    """
    db = Database()
    fd = db.read_records(
        table_name="file_control",
        fields=[
            "brand_id",
            "file_type",
            "file_processing_date",
        ],
        where={"client_id": client_id, "file_id": file_id},
    )
    fd["file_processing_date"] = pd.to_datetime(
        fd["file_processing_date"], format="%Y-%m-%d"
    ).dt.date
    return fd.iloc[0]


def _get_visa_ardef(file_date: date) -> pd.DataFrame:
    """
    Return a dataframe of Visa ARDEF records valid for the file_id's date.
    """
    db = Database()
    fd = db.read_records(
        table_name="visa_ardef",
        fields=[
            "low_key_for_range",
            "table_key",
            "effective_date",
            "valid_until",
            "account_funding_source",
            "ardef_country",
            "ardef_region",
            "b2b_program_id",
            "country",
            "fast_funds",
            "nnss_indicator",
            "product_id",
            "product_subtype",
            "region",
            "technology_indicator",
            "travel_indicator",
        ],
        where={"delete_indicator": " "},
    )
    # Clean integer fields.
    int_cols = ["low_key_for_range", "table_key"]
    fd[int_cols] = fd[int_cols].apply(
        pd.to_numeric, downcast="integer", errors="coerce"
    )
    # Clean date fields and default empty "valid_until" to file's date.
    date_cols = ["effective_date", "valid_until"]
    for col in date_cols:
        fd[col] = pd.to_datetime(fd[col], format="%Y-%m-%d", errors="coerce").dt.date
    fd["valid_until"] = fd["valid_until"].fillna(file_date)
    # Filter out ranges that are not valid for the file's date.
    fd = fd[(file_date >= fd["effective_date"]) & (file_date <= fd["valid_until"])]
    # Remove duplicate keys and ranges that overlap with a previous range.
    fd = fd.sort_values(
        ["table_key", "effective_date", "low_key_for_range"],
        ascending=[True, False, True],
    )
    fd = fd.drop_duplicates(subset="table_key", keep="first")
    fd = fd.drop_duplicates(subset="low_key_for_range", keep="first")
    fd["previous_table_key"] = fd["table_key"].shift(1)
    fd["overlap"] = fd["low_key_for_range"] <= fd["previous_table_key"]
    fd = fd[~fd["overlap"]].drop(columns=["previous_table_key", "overlap"])
    # Add an interval column to facilitate merge with transactions.
    fd["account_interval"] = pd.IntervalIndex.from_tuples(
        list(zip(fd["low_key_for_range"], fd["table_key"])), closed="both"
    )
    return fd.reset_index(drop=True)


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
    log.logger.info(f"Reading additional metadata for {client_id} file {file_id}")
    client_data = _get_client_data(client_id)
    file_data = _get_file_data(client_id, file_id)
    ardef_data = _get_visa_ardef(file_data["file_processing_date"])
    log.logger.info(f"Calculating additional fields from {client_id} file {file_id}")
    data["account_interval"] = pd.cut(
        data["account_number"].str.replace("*", "0").str.slice(0, 9).astype(int),
        ardef_data["account_interval"],
        include_lowest=True,
    )
    fill_interval = pd.Interval(left=0, right=0, closed="both")
    data["account_interval"] = data["account_interval"].cat.add_categories(
        [fill_interval]
    )
    data["account_interval"] = data["account_interval"].where(
        data["account_interval"].notna(), fill_interval
    )
    BASEII_FIELDS: list[Type[CalculatedField]] = [
        ardef_country,
        authorization_code_valid,
        b2b_program_id,
        business_application_id,
        business_format_code,
        business_mode,
        business_transaction_type,
        fast_funds,
        funding_source,
        issuer_bin_8,
        issuer_country,
        issuer_region,
        jurisdiction_assigned,
        jurisdiction_country,
        jurisdiction_region,
        jurisdiction,
        message_reason_code,
        network_identification_code,
        nnss_indicator,
        product_id,
        product_subtype,
        reversal_indicator,
        source_currency_code_alphabetic,
        surcharge_amount,
        technology_indicator,
        timeliness,
        travel_indicator,
        type_of_purchase,
    ]
    fields = []
    for field in BASEII_FIELDS:
        calculated_field = field(
            client_data,
            file_data,
            ardef_data,
        ).calculate(data, type_record="draft")
        calculated_field.name = field.__name__
        fields.append(calculated_field)
    calculated_df = pd.concat(fields, axis=1)
    log.logger.info(
        f"Saving Visa Draft calculated fields from {client_id} file {file_id}"
    )
    fs.write_parquet(
        calculated_df, target_layer, client_id, file_id, subdir=target_subdir
    )


def calculate_sms_fields(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    origin_subdir="300-SMS_CLN_MESSAGES",
    target_subdir="400-SMS_CAL_MESSAGES",
) -> None:
    """
    Calculate additional fields from clean SMS transaction data.
    """
    log.logger.info(f"Reading clean SMS Transactions from {client_id} file {file_id}")
    data = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=origin_subdir,
    )
    log.logger.info(f"Reading additional metadata for {client_id} file {file_id}")
    client_data = _get_client_data(client_id)
    file_data = _get_file_data(client_id, file_id)
    ardef_data = _get_visa_ardef(file_data["file_processing_date"])
    log.logger.info(f"Calculating additional fields from {client_id} file {file_id}")
    data["account_interval"] = pd.cut(
        data["card_number"].str.replace("*", "0").str.slice(0, 9).astype(int),
        ardef_data["account_interval"],
        include_lowest=True,
    )
    fill_interval = pd.Interval(left=0, right=0, closed="both")
    data["account_interval"] = data["account_interval"].cat.add_categories(
        [fill_interval]
    )
    data["account_interval"] = data["account_interval"].where(
        data["account_interval"].notna(), fill_interval
    )
    SMS_FIELDS: list[Type[CalculatedField]] = [
        acquirer_bin,
        ardef_country,
        authorization_code_valid,
        b2b_program_id,
        business_mode,
        business_transaction_type,
        fast_funds,
        funding_source,
        issuer_bin_8,
        issuer_country,
        issuer_region,
        jurisdiction,
        jurisdiction_assigned,
        jurisdiction_country,
        jurisdiction_region,
        nnss_indicator,
        processing_code_transaction_type,
        product_id,
        product_subtype,
        reversal_indicator,
        source_amount,
        source_currency_code_alphabetic,
        technology_indicator,
        timeliness,
        transaction_code_sms,
        travel_indicator,
    ]
    fields = []
    for field in SMS_FIELDS:
        calculated_field = field(
            client_data,
            file_data,
            ardef_data,
        ).calculate(data, type_record="sms")
        calculated_field.name = field.__name__
        fields.append(calculated_field)
    calculated_df = pd.concat(fields, axis=1)
    log.logger.info(
        f"Saving Visa SMS calculated fields from {client_id} file {file_id}"
    )
    fs.write_parquet(
        calculated_df, target_layer, client_id, file_id, subdir=target_subdir
    )


def calculate_vss_fields() -> None:
    raise NotImplementedError
