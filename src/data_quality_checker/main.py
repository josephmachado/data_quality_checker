import os
from typing import Any, Literal

import duckdb

from data_quality_checker.connector.output_log import DBConnector


class DataQualityChecker:
    def __init__(self, db_connector: DBConnector) -> None:
        self.db_connector = db_connector

    def _validate_path_exists(self, data_path: str) -> None:
        """Helper to validate that a file path exists and is readable by DuckDB"""
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Data path not found: {data_path}")
        try:
            # Check if DuckDB can at least parse the header/schema
            duckdb.sql(f"SELECT * FROM '{data_path}' LIMIT 0")
        except Exception as e:
            raise ValueError(f"Data path is not readable by DuckDB: {data_path}. Error: {str(e)}")

    def is_column_unique(self, data_path: str, unique_column: str) -> bool:
        """Function to check if the `unique_column` in the file at `data_path` is unique

        Args:
            data_path (str): Path to the data file to be validated
            unique_column (str): The name of the column to be checked for uniqueness

        Returns:
            bool: True if the column is unique
        """
        self._validate_path_exists(data_path)
        # SQL returns rows where duplicates exist (0 rows = success)
        subquery = f"SELECT {unique_column} FROM '{data_path}' GROUP BY {unique_column} HAVING COUNT(*) > 1"
        error_count_row = duckdb.sql(f"SELECT COUNT(*) FROM ({subquery})").fetchone()
        error_count = error_count_row[0] if error_count_row is not None else 0

        result = error_count == 0
        self.log_results(
            "is_column_unique",
            result,
            column=unique_column,
            data_path=data_path,
            error_count=error_count,
        )
        return result

    def is_column_not_null(self, data_path: str, not_null_column: str) -> bool:
        """Function to check if the `not_null_column` in the file at `data_path` is not null

        Args:
            data_path (str): Path to the data file to be validated
            not_null_column (str): The name of the column to be checked for not null

        Returns:
            bool: True if the column is not null
        """
        self._validate_path_exists(data_path)
        # SQL returns rows where values are null (0 rows = success)
        subquery = f"SELECT * FROM '{data_path}' WHERE {not_null_column} IS NULL"
        query_result = duckdb.sql(f"SELECT COUNT(*) FROM ({subquery})").fetchone()
        error_count = query_result[0] if query_result else 0

        result = error_count == 0
        self.log_results(
            "is_column_not_null",
            result,
            column=not_null_column,
            data_path=data_path,
            error_count=error_count,
        )
        return result

    def is_column_enum(
        self,
        data_path: str,
        enum_column: str,
        enum_values: list[str],
    ) -> bool:
        """Function to check if a column only has accepted values

        Args:
            data_path (str): Path to the data file to be validated
            enum_column (str): The column to be checked if it only has the accepted values
            enum_values (list[str]): The list of accepted values

        Returns:
            bool: True if column only has the accepted values
        """
        self._validate_path_exists(data_path)
        enum_vals_str = ", ".join([f"'{v}'" for v in enum_values])
        # SQL returns rows where values are not in enum (0 rows = success)
        subquery = f"SELECT {enum_column} FROM '{data_path}' WHERE {enum_column} NOT IN ({enum_vals_str}) AND {enum_column} IS NOT NULL"
        error_count_row = duckdb.sql(f"SELECT COUNT(*) FROM ({subquery})").fetchone()
        error_count = error_count_row[0] if error_count_row is not None else 0

        result = error_count == 0
        self.log_results(
            "is_column_enum",
            result,
            column=enum_column,
            enum_values=enum_values,
            data_path=data_path,
            error_count=error_count,
        )
        return result

    def are_tables_referential_integral(
        self,
        data_path: str,
        reference_path: str,
        join_keys: list[str],
    ) -> bool:
        """Function to check for referential integrity between data files

        Args:
            data_path (str): Path to the data file that is to be checked
            reference_path (str): Path to the second data file to validate against
            join_keys (list[str]): The join keys for both files

        Returns:
            bool: True if the files have referential integrity based on the join keys
        """
        self._validate_path_exists(data_path)
        self._validate_path_exists(reference_path)

        join_conditions = " AND ".join([f"l.{key} = r.{key}" for key in join_keys])
        where_conditions = " AND ".join([f"r.{key} IS NULL" for key in join_keys])

        # SQL returns orphaned rows (0 rows = success)
        subquery = f"""
            SELECT l.* 
            FROM '{data_path}' l 
            LEFT JOIN '{reference_path}' r ON {join_conditions} 
            WHERE {where_conditions}
        """
        error_count_row = duckdb.sql(f"SELECT COUNT(*) FROM ({subquery})").fetchone()
        error_count = error_count_row[0] if error_count_row is not None else 0

        result = error_count == 0

        self.log_results(
            "are_tables_referential_integral",
            result,
            join_keys=join_keys,
            data_path=data_path,
            reference_path=reference_path,
            error_count=error_count,
        )
        return result

    def is_column_in_data(self, data_path: str, column_name: str) -> bool:
        """Function to check if a column exists in the data file

        Args:
            data_path (str): Path to the data file to be checked
            column_name (str): The name of the column to check for its existence

        Returns:
            bool: True if the column exists in the data file
        """
        if not isinstance(column_name, str):
            raise TypeError(
                f"column_name must be a string, not {type(column_name).__name__}"
            )
        self._validate_path_exists(data_path)

        try:
            # Try to select the column with 0 rows to check existence
            duckdb.sql(f"SELECT {column_name} FROM '{data_path}' LIMIT 0")
            result = True
        except Exception:
            result = False

        self.log_results(
            "is_column_in_data",
            result,
            column=column_name,
            data_path=data_path,
        )
        return result

    def log_results(
        self,
        data_quality_check_type: Literal[
            "is_column_unique",
            "is_column_not_null",
            "is_column_enum",
            "are_tables_referential_integral",
            "is_column_in_data",
        ],
        result: bool,
        **kwargs: Any,
    ) -> None:
        """Function to log results of a data quality check to a log location

        Args:
            data_quality_check_type: Type of data quality check that was performed
            result: The boolean result of the check
            **kwargs: Additional parameters specific to the check
        """
        self.db_connector.log(data_quality_check_type, result, **kwargs)
