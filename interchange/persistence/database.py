import os
import sqlite3

import dotenv
from pandas import DataFrame

from interchange.logs.logger import Logger


log = Logger(__name__)


class Database:
    """
    Class to handle all database connections and CRUD operations.
    """

    def __init__(self) -> None:
        dotenv.load_dotenv()
        self.connection = self._create_connection(
            db_path=os.environ["ITX_DATABASE_PATH"]
        )

    def __del__(self) -> None:
        self._close_connection()

    def _create_connection(self, db_path: str) -> sqlite3.Connection:
        """
        Create a database connection.
        """
        try:
            conn = sqlite3.connect(db_path)
            log.logger.debug("Connected to SQLite database")
        except sqlite3.Error as e:
            log.logger.error(f"Error connecting to database: '{e}'")
        return conn

    def _close_connection(self) -> None:
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.close()
            log.logger.debug("Closed connection to SQLite database")

    def _execute(self, sql_statement: str, commit_option: bool = False) -> list[tuple]:
        """
        Execute the given SQL statement.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql_statement)
            if commit_option:
                self.connection.commit()
            result: list[tuple] = cursor.fetchall()
            cursor.close()
            log.logger.debug("SQL statement executed successfully")
            return result
        except sqlite3.Error as e:
            log.logger.error(f"Error executing SQL statement: '{e}'")
            return []

    def _format_list(self, values: list[str | int | float]) -> list[str]:
        """
        Format a list of values to be used as part of a SQL statement.
        """
        return [f"'{val}'" if isinstance(val, str) else str(val) for val in values]

    def _format_dict(self, values: dict[str, str | int | float]) -> dict[str, str]:
        """
        Format a dictionary of values to be used as part of a SQL statement.
        """
        return {
            fd: f"'{val}'" if isinstance(val, str) else str(val)
            for (fd, val) in values.items()
        }

    def create_table(self, table_name: str, fields_def: dict[str, str]) -> None:
        """
        Create a table with the given table name and field definitions.
        """
        fields_str = ", ".join([f"{fd} {typ}" for (fd, typ) in fields_def.items()])
        sql_statement = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {fields_str}
            );
            """
        log.logger.debug("Attempting to execute CREATE TABLE SQL statement")
        self._execute(sql_statement, commit_option=True)

    def drop_table(self, table_name: str) -> None:
        """
        Drop a table with the given table name.
        """
        sql_statement = f"""
            DROP TABLE IF EXISTS {table_name};
            """
        log.logger.debug("Attempting to execute DROP TABLE SQL statement")
        self._execute(sql_statement, commit_option=True)

    def create_records(
        self, table_name: str, fields: list[str], values: list[list[str | int | float]]
    ) -> None:
        """
        Insert records into a table from the given lists of fields and values.
        """
        fields_str = ", ".join(fields)
        fmt_values = [self._format_list(tp) for tp in values]
        values_str = ", ".join([f"({", ".join(tp)})" for tp in fmt_values])
        sql_statement = f"""
            INSERT INTO {table_name} ({fields_str})
            VALUES {values_str};
            """
        log.logger.debug("Attempting to execute INSERT SQL statement")
        self._execute(sql_statement, commit_option=True)

    def read_records(
        self,
        table_name: str,
        fields: list[str],
        where: dict[str, str | int | float] = {},
    ) -> DataFrame:
        """
        Read records from a table with a list of fields and an optional 'where' clause.
        """
        fields_str = ", ".join(fields)
        sql_statement = f"""
            SELECT {fields_str}
            FROM {table_name}"""
        if where:
            fmt_where = self._format_dict(where)
            where_str = " AND ".join(
                [f"{fd} = {val}" for (fd, val) in fmt_where.items()]
            )
            sql_statement += f"""
                WHERE {where_str}"""
        sql_statement += ";\n"
        log.logger.debug("Attempting to execute SELECT SQL statement")
        result = self._execute(sql_statement)
        result_df = DataFrame(result, columns=fields, dtype=str)
        return result_df

    def update_records(
        self,
        table_name: str,
        updates: dict[str, str | int | float],
        where: dict[str, str | int | float],
    ) -> None:
        """
        Update records on a table with set of update instructions and a 'where' clause.
        """
        fmt_updates = self._format_dict(updates)
        update_str = ", ".join([f"{fd} = {val}" for (fd, val) in fmt_updates.items()])
        fmt_where = self._format_dict(where)
        where_str = " AND ".join([f"{fd} = {val}" for (fd, val) in fmt_where.items()])
        sql_statement = f"""
            UPDATE {table_name}
            SET {update_str}
            WHERE {where_str};
            """
        log.logger.debug("Attempting to execute UPDATE SQL statement")
        self._execute(sql_statement, commit_option=True)

    def delete_records(
        self,
        table_name: str,
        where: dict[str, str | int | float],
    ) -> None:
        """
        Delete records on a table specifying a mandatory 'where' clause.
        """
        fmt_where = self._format_dict(where)
        where_str = " AND ".join([f"{fd} = {val}" for (fd, val) in fmt_where.items()])
        sql_statement = f"""
            DELETE FROM {table_name}
            WHERE {where_str};
            """
        log.logger.debug("Attempting to execute DELETE SQL statement")
        self._execute(sql_statement, commit_option=True)
