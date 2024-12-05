from datetime import date

import numpy as np
import pandas as pd

from interchange.logs.logger import Logger
from interchange.persistence.database import Database
from interchange.persistence.file import FileStorage


log = Logger(__name__)
fs = FileStorage()


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


def _get_visa_rule_definitions(file_date: date, type_record: str) -> pd.DataFrame:
    """
    Get Visa's interchange rule assignment criteria for the file's processing date.
    """
    db = Database()
    df = db.read_records(
        table_name="visa_rules",
        fields=[
            "region_country_code",
            "valid_from",
            "valid_until",
            "intelica_id",
            "fee_descriptor",
            "fee_currency",
            "fee_variable",
            "fee_fixed",
            "fee_min",
            "fee_cap",
            "business_mode",
            "issuer_country",
            "issuer_region",
            "technology_indicator",
            "product_id",
            "fast_funds",
            "travel_indicator",
            "b2b_program_id",
            "account_funding_source",
            "nnss_indicator",
            "product_subtype",
            "transaction_code",
            "transaction_code_qualifier",
            "issuer_bin_8",
            "acquirer_bin",
            "acquirer_business_id",
            "transaction_amount_currency",
            "transaction_amount",
            "acquirer_country",
            "acquirer_region",
            "merchant_country_code",
            "merchant_country_region",
            "merchant_category_code",
            "requested_payment_service",
            "usage_code",
            "authorization_characteristics_indicator",
            "authorization_code",
            "pos_terminal_capability",
            "cardholder_id_method",
            "pos_entry_mode",
            "timeliness",
            "reimbursement_attribute",
            "special_condition_indicator",
            "fee_program_indicator",
            "moto_eci_indicator",
            "acceptance_terminal_indicator",
            "prepaid_card_indicator",
            "pos_environment_code",
            "business_format_code",
            "business_application_id",
            "type_purchase",
            "network_identification_code",
            "message_reason_code",
            "surcharge_amount",
            "authorized_amount",
            "authorization_response_code",
            "merchant_verification_value",
            "dynamic_currency_conversion_indicator",
            "cvv2_result_code",
            "national_tax_indicator",
            "merchant_vat",
            "summary_commodity",
        ],
    )
    int_cols = ["intelica_id"]
    numeric_cols = ["fee_variable", "fee_fixed", "fee_min", "fee_cap"]
    date_cols = ["valid_from", "valid_until"]
    df[int_cols] = df[int_cols].apply(
        pd.to_numeric, downcast="integer", errors="coerce"
    )
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col].str.slice(0, 10), format="%Y-%m-%d", errors="coerce"
        ).dt.date
    df[date_cols] = df[date_cols].fillna(date.today())
    df_valid = df[(file_date >= df["valid_from"]) & (file_date <= df["valid_until"])]
    df_valid = df_valid.sort_values(["region_country_code", "intelica_id"])
    match type_record:
        case "draft":
            df_valid.drop(
                columns=[
                    "acquirer_country",
                    "acquirer_region",
                ],
                inplace=True,
            )
            df_valid.rename(
                columns={
                    "account_funding_source": "funding_source",
                    "acquirer_bin": "account_reference_number_acquiring_identifier",
                    "cvv2_result_code": "cvv_result_code",
                    "dynamic_currency_conversion_indicator": "dcc_indicator",
                    "merchant_country_code": "jurisdiction_country",
                    "merchant_country_region": "jurisdiction_region",
                    "merchant_vat": "merchant_vat_registration_number",
                    "moto_eci_indicator": "moto_ec_indicator",
                    "national_tax_indicator": "national_tax_included",
                    "pos_environment_code": "pos_environment",
                    "pos_terminal_capability": "pos_terminal_capacity",
                    "special_condition_indicator": "special_condition_indicator_merchant_draft_indicator",
                    "summary_commodity": "summary_commodity_code",
                    "transaction_amount_currency": "source_currency_code",
                    "transaction_amount": "source_amount",
                    "transaction_code_qualifier": "draft_code_qualifier_0",
                    "transaction_code": "draft_code",
                    "type_purchase": "type_of_purchase",
                },
                inplace=True,
            )
            return df_valid
        case _:
            raise NotImplementedError


def calculate_baseii_interchange(
    origin_layer: FileStorage.Layer,
    target_layer: FileStorage.Layer,
    client_id: str,
    file_id: str,
    transactions_subdir="300-BASEII_CLN_DRAFTS",
    calculated_subdir="400-BASEII_CAL_DRAFTS",
    target_subdir="500-BASEII_ITX_DRAFTS",
) -> None:
    """
    Calculate interchange fee fields for BASE II transaction data.
    """
    log.logger.info(
        f"Reading clean BASE II Transactions from {client_id} file {file_id}"
    )
    transactions = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=transactions_subdir,
    )
    log.logger.info(f"Reading calculated field data from {client_id} file {file_id}")
    calculated = fs.read_parquet(
        origin_layer,
        client_id,
        file_id,
        subdir=calculated_subdir,
    )
    log.logger.info(
        f"Merging transactional and calculated data from {client_id} file {file_id}"
    )
    data = transactions.join(calculated, how="left", lsuffix="_baseii")

    # TEMPORARY:
    log.logger.info(f"Reading visa rule definitions for {client_id} file {file_id}")
    file_data = _get_file_data(client_id, file_id)
    rules_data = _get_visa_rule_definitions(
        file_data["file_processing_date"], type_record="draft"
    )


def calculate_sms_interchange() -> None:
    raise NotImplementedError


# calculate_baseii_interchange(
#     fs.Layer.STAGING, fs.Layer.STAGING, "DEMO", "CDA26F0BEB4349D03346A721DDCF0DC7"
# )
