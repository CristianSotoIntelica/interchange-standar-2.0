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
