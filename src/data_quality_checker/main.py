from typing import Any, Literal

import polars as pl

from data_quality_checker.connector.output_log import DBConnector


class DataQualityChecker:
    def __init__(self, db_connector: DBConnector):
        self.db_connector = db_connector

    def is_column_unique(
        self, data_frame_to_validate: pl.DataFrame, unique_column: str
    ) -> bool:
        """Function to check if the `unique_column` in the `data_frame_to_validate` is unique

        Args:
            data_frame_to_validate (pl.DataFrame): A polars dataframe whose column is to be validated
            unique_column (str): The name of the column to be checked for uniqueness

        Returns:
            bool: True if the column is unique
        """
        total_rows = data_frame_to_validate.height
        unique_rows = data_frame_to_validate.select(unique_column).n_unique()

        result = total_rows == unique_rows
        self.log_results(
            "is_column_unique",
            result,
            column=unique_column,
            total_rows=total_rows,
            unique_rows=unique_rows,
        )
        return result

    def is_column_not_null(
        self, data_frame_to_validate: pl.DataFrame, not_null_column: str
    ) -> bool:
        """Function to check if the `not_null_column` in the `data_frame_to_validate` is not null

        Args:
            data_frame_to_validate (pl.DataFrame): A polars dataframe whose column is to be validated
            not_null_column (str): The name of the column to be checked for not null

        Returns:
            bool: True if the column is not null
        """
        null_count = data_frame_to_validate.select(not_null_column).null_count().item()

        result = null_count == 0
        self.log_results(
            "is_column_not_null",
            result,
            column=not_null_column,
            null_count=null_count,
            total_rows=data_frame_to_validate.height,
        )
        return result

    def is_column_enum(
        self,
        data_frame_to_validate: pl.DataFrame,
        enum_column: str,
        enum_values: list[str],
    ) -> bool:
        """Function to check if a column only has accepted values

        Args:
            data_frame_to_validate (pl.DataFrame): A polars dataframe whose column is to be validated
            enum_column (str): The column to be checked if it only has the accepted values
            enum_values (list[str]): The list of accepted values

        Returns:
            bool: True if column only has the accepted values
        """
        unique_values = (
            data_frame_to_validate.select(enum_column).unique().to_series().to_list()
        )

        # Remove None/null values from unique_values for comparison
        unique_values_clean = [v for v in unique_values if v is not None]

        # Check if all unique values are in the enum_values list
        result = all(value in enum_values for value in unique_values_clean)

        invalid_values = [v for v in unique_values_clean if v not in enum_values]

        self.log_results(
            "is_column_enum",
            result,
            column=enum_column,
            enum_values=enum_values,
            invalid_values=invalid_values if invalid_values else None,
        )
        return result

    def are_tables_referential_integral(
        self,
        data_frame_to_validate: pl.DataFrame,
        data_frame_to_validate_against: pl.DataFrame,
        join_keys: list[str],
    ) -> bool:
        """Function to check for referential integrity between dataframes

        Args:
            data_frame_to_validate (pl.DataFrame): A dataframe that is to be checked for referential integrity
            data_frame_to_validate_against (pl.DataFrame): A second dataframe that is to be checked for referential integrity
            join_keys (list[str]): The left and right join keys for data_frame_to_validate and data_frame_to_validate_against dataframes respectively

        Returns:
            bool: True if the dataframes have referential integrity based on the join keys
        """
        # Count rows where join keys from the right table are null (indicating missing references)
        # Check if all rows from left table exist in right table
        original_count = data_frame_to_validate.height

        # Perform inner join to get matching rows
        inner_joined = data_frame_to_validate.join(
            data_frame_to_validate_against.select(join_keys), on=join_keys, how="inner"
        )

        matched_count = inner_joined.height

        result = original_count == matched_count

        self.log_results(
            "are_tables_referential_integral",
            result,
            join_keys=join_keys,
            total_rows=original_count,
            matched_rows=matched_count,
            orphaned_rows=original_count - matched_count,
        )
        return result

    def log_results(
        self,
        data_quality_check_type: Literal[
            "is_column_unique",
            "is_column_not_null",
            "is_column_enum",
            "are_tables_referential_integral",
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
