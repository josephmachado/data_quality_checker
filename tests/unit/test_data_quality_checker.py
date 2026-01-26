import pytest

from data_quality_checker.main import DataQualityChecker

from data_quality_checker.connector.output_log import DBConnector


class TestDataQualityChecker:
    @pytest.fixture
    def checker(self, mock_db_connector: DBConnector) -> DataQualityChecker:
        """Create a DataQualityChecker instance with mocked db_connector"""
        return DataQualityChecker(mock_db_connector)

    # Tests for is_column_unique

    def test_is_column_unique_returns_true_when_column_is_unique(
        self, checker: DataQualityChecker, mock_db_connector: DBConnector, unique_data_path: str
    ) -> None:
        """Test that is_column_unique returns True when all values are unique"""
        result = checker.is_column_unique(unique_data_path, "id")

        assert result is True
        mock_db_connector.log.assert_called_once() # type: ignore
        call_args = mock_db_connector.log.call_args  # type: ignore
        assert call_args[0][0] == "is_column_unique"
        assert call_args[0][1] is True
        assert call_args[1]["column"] == "id"
        assert call_args[1]["data_path"] == unique_data_path

    def test_is_column_unique_returns_false_when_column_has_duplicates(
            self,  checker: DataQualityChecker, mock_db_connector: DBConnector, duplicate_data_path: str
    ) -> None:
        """Test that is_column_unique returns False when there are duplicate values"""
        result = checker.is_column_unique(duplicate_data_path, "id") # type: ignore

        assert result is False
        mock_db_connector.log.assert_called_once() # type: ignore
        call_args = mock_db_connector.log.call_args # type: ignore
        assert call_args[0][0] == "is_column_unique"
        assert call_args[0][1] is False

    def test_is_column_unique_with_empty_file(
        self, checker, mock_db_connector, empty_data_path
    ) -> None:
        """Test that is_column_unique handles files with only headers (effectively empty)"""
        result = checker.is_column_unique(empty_data_path, "id")

        assert result is True
        mock_db_connector.log.assert_called_once()

    # Tests for is_column_not_null

    def test_is_column_not_null_returns_true_when_no_nulls(
        self, checker, mock_db_connector, no_nulls_path
    ) -> None:
        """Test that is_column_not_null returns True when column has no null values"""
        result = checker.is_column_not_null(no_nulls_path, "name")

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_not_null"
        assert call_args[0][1] is True
        assert call_args[1]["column"] == "name"

    def test_is_column_not_null_returns_false_when_nulls_exist(
        self, checker, mock_db_connector, has_nulls_path
    ) -> None:
        """Test that is_column_not_null returns False when column has null values"""
        result = checker.is_column_not_null(has_nulls_path, "name")

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_not_null"
        assert call_args[0][1] is False
        assert call_args[1]["error_count"] == 1

    # Tests for is_column_enum

    def test_is_column_enum_returns_true_when_all_values_valid(
        self, checker, mock_db_connector, valid_enum_path
    ) -> None:
        """Test that is_column_enum returns True when all values are in enum list"""
        result = checker.is_column_enum(
            valid_enum_path, "status", ["active", "inactive", "pending"]
        )

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_enum"
        assert call_args[0][1] is True

    def test_is_column_enum_returns_false_when_invalid_values_exist(
        self, checker, mock_db_connector, invalid_enum_path
    ) -> None:
        """Test that is_column_enum returns False when values outside enum list exist"""
        result = checker.is_column_enum(
            invalid_enum_path, "status", ["active", "inactive", "pending"]
        )

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_enum"
        assert call_args[0][1] is False

    def test_is_column_enum_ignores_null_values(
        self, checker, mock_db_connector, null_enum_path
    ) -> None:
        """Test that is_column_enum ignores null values in validation"""
        result = checker.is_column_enum(
            null_enum_path, "status", ["active", "inactive", "pending"]
        )

        assert result is True
        mock_db_connector.log.assert_called_once()

    # Tests for are_tables_referential_integral

    def test_are_tables_referential_integral_returns_true_when_all_references_exist(
        self, checker, mock_db_connector, orders_data_path, users_data_path
    ) -> None:
        """Test that are_tables_referential_integral returns True when all foreign keys exist"""
        result = checker.are_tables_referential_integral(
            orders_data_path, users_data_path, ["user_id"]
        )

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "are_tables_referential_integral"
        assert call_args[0][1] is True
        assert call_args[1]["error_count"] == 0

    def test_are_tables_referential_integral_returns_false_when_orphaned_records_exist(
        self, checker, mock_db_connector, orphaned_orders_path, users_data_path
    ) -> None:
        """Test that are_tables_referential_integral returns False when orphaned records exist"""
        result = checker.are_tables_referential_integral(
            orphaned_orders_path, users_data_path, ["user_id"]
        )

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "are_tables_referential_integral"
        assert call_args[0][1] is False
        assert call_args[1]["error_count"] == 1

    # Tests for is_column_in_data

    def test_is_column_in_data_returns_true_when_column_exists(
        self, checker, mock_db_connector, unique_data_path
    ) -> None:
        """Test that is_column_in_data returns True when the column exists"""
        result = checker.is_column_in_data(unique_data_path, "id")

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_in_data"
        assert call_args[0][1] is True
        assert call_args[1]["column"] == "id"

    def test_is_column_in_data_returns_false_when_column_does_not_exist(
        self, checker, mock_db_connector, unique_data_path
    ) -> None:
        """Test that is_column_in_data returns False when the column does not exist"""
        result = checker.is_column_in_data(unique_data_path, "age")

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_in_data"
        assert call_args[0][1] is False
        assert call_args[1]["column"] == "age"

    def test_is_column_in_data_raises_type_error_when_column_name_not_string(
        self, checker, unique_data_path
    ) -> None:
        """Test that is_column_in_data raises TypeError when column_name is not a string"""
        with pytest.raises(TypeError, match="column_name must be a string"):
            checker.is_column_in_data(unique_data_path, 123)

    # Tests for _validate_path_exists

    def test_validate_path_exists_raises_file_not_found(self, checker) -> None:
        """Test that _validate_path_exists raises FileNotFoundError for non-existent path"""
        with pytest.raises(FileNotFoundError, match="Data path not found"):
            checker.is_column_unique("non_existent.csv", "id")

    def test_validate_path_exists_raises_value_error_for_unreadable_file(
        self, checker, corrupt_data_path
    ) -> None:
        """Test that _validate_path_exists raises ValueError when DuckDB cannot read the file"""
        with pytest.raises(ValueError, match="is not readable by DuckDB"):
            checker.is_column_unique(corrupt_data_path, "id")

    # Tests for log_results

    def test_log_results_calls_db_connector_log(self, checker, mock_db_connector) -> None:
        """Test that log_results properly delegates to db_connector.log"""
        checker.log_results(
            "is_column_unique", True, column="test_column", extra_info="test"
        )

        mock_db_connector.log.assert_called_once_with(
            "is_column_unique", True, column="test_column", extra_info="test"
        )
