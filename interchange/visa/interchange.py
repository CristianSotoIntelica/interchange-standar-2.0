from datetime import date

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


def _apply_condition_default(
    condition: str, rule: pd.Series, batch: pd.DataFrame
) -> pd.DataFrame:
    return batch


def _apply_condition_space(
    condition: str, rule: pd.Series, batch: pd.DataFrame
) -> pd.DataFrame:
    return batch


def _apply_condition_greater_less(
    condition: str, rule: pd.Series, batch: pd.DataFrame
) -> pd.DataFrame:
    return batch


def _apply_condition_number_between(
    condition: str, rule: pd.Series, batch: pd.DataFrame
) -> pd.DataFrame:
    return batch


def _apply_condition_amount_currency(
    condition: str, rule: pd.Series, batch: pd.DataFrame
) -> pd.DataFrame:
    return batch


def _apply_condition(
    condition: str, rule: pd.Series, batch: pd.DataFrame
) -> pd.DataFrame:
    """
    Check what type of condition to apply to a batch and apply the condition.
    """
    column_group_space = [
        "nnss_indicator",
        "cardholder_id_method",
        "moto_ec_indicator",
        "acceptance_terminal_indicator",
        "merchant_vat_registration_number",
    ]
    column_group_greater_less = [
        "surcharge_amount",
        "timeliness",
    ]
    column_group_number_between = [
        "merchant_category_code",
        "issuer_bin_8",
    ]
    column_group_amount_currency = [
        "source_amount",
    ]
    match condition:
        case col if col in column_group_space:
            result = _apply_condition_space(condition, rule, batch)
        case col if col in column_group_greater_less:
            result = _apply_condition_greater_less(condition, rule, batch)
        case col if col in column_group_number_between:
            result = _apply_condition_number_between(condition, rule, batch)
        case col if col in column_group_amount_currency:
            result = _apply_condition_amount_currency(condition, rule, batch)
        case _:
            result = _apply_condition_default(condition, rule, batch)

    return result


def _evaluate_interchange_fees(
    transactions: pd.DataFrame, rules: pd.DataFrame
) -> pd.DataFrame:
    """
    Evaluate interchange fee criteria for a dataset of transactions.
    """
    # Filter rule definitions to only jurisdictions present in data.
    jurisdiction_list = transactions["jurisdiction_assigned"].unique()
    rules_to_evaluate = rules[rules["region_country_code"].isin(jurisdiction_list)]
    # Initialize rule identifier fields.
    transactions["interchange_region_country_code"] = ""
    transactions["interchange_intelica_id"] = -1
    transactions["interchange_fee_descriptor"] = ""
    transactions["interchange_fee_currency"] = ""
    transactions["interchange_fee_variable"] = 0.0
    transactions["interchange_fee_fixed"] = 0.0
    transactions["interchange_fee_min"] = 0.0
    transactions["interchange_fee_cap"] = 0.0
    # Iterate through each rule definition.
    conditions_to_skip = [
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
        "source_currency_code",
    ]
    update_columns = [
        "region_country_code",
        "intelica_id",
        "fee_descriptor",
        "fee_currency",
        "fee_variable",
        "fee_fixed",
        "fee_min",
        "fee_cap",
    ]
    conditions = [c for c in rules_to_evaluate.columns if c not in conditions_to_skip]
    for _, rule in rules_to_evaluate.iterrows():
        # Step 1: Filter unprocessed transactions and decide to break, skip or evaluate.
        next_batch = transactions[transactions["interchange_intelica_id"] == -1]
        if next_batch.empty:
            break
        next_batch = next_batch[
            next_batch["jurisdiction_assigned"] == rule["region_country_code"]
        ]
        if next_batch.empty:
            continue
        # Step 2: Iterate through each condition in the rule and apply its condition.
        for condition in conditions:
            next_batch = _apply_condition(condition, rule, next_batch)
        # Step 3: Update transaction table with batch results.
        for column in update_columns:
            next_batch.loc[:, f"interchange_{column}"] = rule[column]
        transactions.update(next_batch[[f"interchange_{c}" for c in update_columns]])

    columns_to_return = [f"interchange_{c}" for c in update_columns]
    columns_to_return = ["source_currency_code", "source_amount"] + columns_to_return
    return transactions[columns_to_return]


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
    merged_data = transactions.join(calculated, how="left", lsuffix="_baseii")

    log.logger.info(f"Reading visa rule definitions for {client_id} file {file_id}")
    file_data = _get_file_data(client_id, file_id)
    rules_data = _get_visa_rule_definitions(
        file_data["file_processing_date"], type_record="draft"
    )

    log.logger.info(f"Evaluating rule definitions for {client_id} file {file_id}")
    interchange_criteria = _evaluate_interchange_fees(merged_data, rules_data)

    log.logger.info(f"Evaluating rule definitions for {client_id} file {file_id}")
    interchange_df = interchange_criteria

    log.logger.info(f"Saving Visa interchange fields for {client_id} file {file_id}")
    fs.write_parquet(
        interchange_df, target_layer, client_id, file_id, subdir=target_subdir
    )


def calculate_sms_interchange() -> None:
    raise NotImplementedError


calculate_baseii_interchange(
    fs.Layer.STAGING, fs.Layer.STAGING, "DEMO", "CDA26F0BEB4349D03346A721DDCF0DC7"
)
