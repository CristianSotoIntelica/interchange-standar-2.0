"""
Procedure to create the database schema for the demo SQLite database.
"""

from interchange.logs.logger import Logger
from interchange.persistence.database import Database


log = Logger(__name__)


if __name__ == "__main__":
    db = Database()

    log.logger.info("Creating 'client' table")
    db.create_table(
        table_name="client",
        fields_def={
            "client_id": "TEXT",
            "client_name": "TEXT",
            "file_mc_block_in": "BOOLEAN",
            "file_mc_block_out": "BOOLEAN",
            "file_mc_encoding_in": "TEXT",
            "file_mc_encoding_out": "TEXT",
            "file_iar_block": "BOOLEAN",
            "file_iar_encoding": "TEXT",
            "local_currency_code": "TEXT",
            "settlement_currency_code": "TEXT",
            "report_currency_code": "TEXT",
            "issuing_bins_6_digits": "TEXT",
            "issuing_bins_8_digits": "TEXT",
            "acquiring_bins": "TEXT",
            "customer_country": "TEXT",
            "duplicate_on_us_flag_visa": "BOOLEAN",
            "duplicate_on_us_flag_mastercard": "BOOLEAN",
        },
    )

    log.logger.info("Creating 'country' table")
    db.create_table(
        table_name="country",
        fields_def={
            "country_numeric": "TEXT",
            "country_code": "TEXT",
            "country_code_alternative": "TEXT",
            "country_name": "TEXT",
            "visa_region_code": "TEXT",
            "mastercard_region_code": "TEXT",
            "legacy_country_id": "INT",
        },
    )

    log.logger.info("Creating 'file_control' table")
    db.create_table(
        table_name="file_control",
        fields_def={
            "client_id": "TEXT",
            "file_id": "TEXT",
            "brand_id": "TEXT",
            "file_type": "TEXT",
            "landing_file_name": "TEXT",
            "landing_parent_zip_name": "TEXT",
            "file_processing_date": "DATE",
            "total_records": "INTEGER",
            "control_status": "TEXT",
            "initial_pid": "TEXT",
            "initial_tid": "TEXT",
            "process_start_ts": "DATETIME ",
            "process_finish_ts": "DATETIME",
        },
    )

    log.logger.info("Creating 'visa_ardef' table")
    db.create_table(
        table_name="visa_ardef",
        fields_def={
            "file_id": "TEXT",
            "row_id": "INT",
            "row_full_data": "TEXT",
            "file_header_date": "DATE",
            "table_type": "TEXT",
            "table_mnemonic": "TEXT",
            "record_type": "TEXT",
            "table_key": "TEXT",
            "low_key_for_range": "TEXT",
            "delete_indicator": "TEXT",
            "effective_date": "DATE",
            "valid_until": "DATE",
            "account_funding_source": "TEXT",
            "account_level_processing_indicator": "TEXT",
            "account_number_length": "INT",
            "account_restricted_use": "TEXT",
            "alternate_atm": "TEXT",
            "ardef_country": "TEXT",
            "ardef_region": "TEXT",
            "b2b_program_id": "TEXT",
            "base_ii_cib": "TEXT",
            "check_digit_algorithm": "TEXT",
            "combo_card": "TEXT",
            "commercial_card_electronic_vat_evidence_indicator": "TEXT",
            "commercial_card_level_2_data_indicator": "TEXT",
            "commercial_card_level_3_enhanced_data_indicator": "TEXT",
            "commercial_card_pos_prompting_indicator": "TEXT",
            "country": "TEXT",
            "domain": "TEXT",
            "fast_funds": "TEXT",
            "issuer_identifier": "TEXT",
            "large_ticket": "TEXT",
            "nnss_indicator": "TEXT",
            "original_credit": "TEXT",
            "original_credit_money_transfer": "TEXT",
            "original_credit_online_gambling": "TEXT",
            "prepaid_program_indicator": "TEXT",
            "product_id": "TEXT",
            "product_subtype": "TEXT",
            "region": "TEXT",
            "settlement_match": "TEXT",
            "technology_indicator": "TEXT",
            "token_indicator": "TEXT",
            "travel_account_data": "TEXT",
            "travel_indicator": "TEXT",
            "row_creation_timestamp": "DATETIME",
        },
    )

    log.logger.info("Creating 'visa_fields' table")
    db.create_table(
        table_name="visa_fields",
        fields_def={
            "type_record": "TEXT",
            "tcsn": "TEXT",
            "position": "INT",
            "length": "INT",
            "column_name": "TEXT",
            "secondary_identifier_pos": "INT",
            "secondary_identifier_len": "INT",
            "secondary_identifier": "TEXT",
            "column_type": "TEXT",
            "float_decimals": "INT",
            "date_format": "TEXT",
        },
    )
