import polars as pl
import pytest

from data_quality_checker.main import DataQualityChecker


class TestDataQualityChecker:
    @pytest.fixture
    def checker(self, mock_db_connector):
        """Create a DataQualityChecker instance with mocked db_connector"""
        return DataQualityChecker(mock_db_connector)

    # Tests for is_column_unique

    def test_is_column_unique_returns_true_when_column_is_unique(
        self, checker, mock_db_connector
    ):
        """Test that is_column_unique returns True when all values are unique"""
        df = pl.DataFrame(
            {"id": [1, 2, 3, 4, 5], "name": ["Alice", "Bob", "Charlie", "David", "Eve"]}
        )

        result = checker.is_column_unique(df, "id")

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_unique"
        assert call_args[0][1] is True
        assert call_args[1]["column"] == "id"

    def test_is_column_unique_returns_false_when_column_has_duplicates(
        self, checker, mock_db_connector
    ):
        """Test that is_column_unique returns False when there are duplicate values"""
        df = pl.DataFrame(
            {"id": [1, 2, 3, 2, 5], "name": ["Alice", "Bob", "Charlie", "David", "Eve"]}
        )

        result = checker.is_column_unique(df, "id")

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_unique"
        assert call_args[0][1] is False

    def test_is_column_unique_with_empty_dataframe(self, checker, mock_db_connector):
        """Test that is_column_unique handles empty dataframes"""
        df = pl.DataFrame({"id": []})

        result = checker.is_column_unique(df, "id")

        assert result is True
        mock_db_connector.log.assert_called_once()

    # Tests for is_column_not_null

    def test_is_column_not_null_returns_true_when_no_nulls(
        self, checker, mock_db_connector
    ):
        """Test that is_column_not_null returns True when column has no null values"""
        df = pl.DataFrame(
            {"id": [1, 2, 3, 4, 5], "name": ["Alice", "Bob", "Charlie", "David", "Eve"]}
        )

        result = checker.is_column_not_null(df, "name")

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_not_null"
        assert call_args[0][1] is True
        assert call_args[1]["column"] == "name"
        assert call_args[1]["null_count"] == 0

    def test_is_column_not_null_returns_false_when_nulls_exist(
        self, checker, mock_db_connector
    ):
        """Test that is_column_not_null returns False when column has null values"""
        df = pl.DataFrame(
            {"id": [1, 2, 3, 4, 5], "name": ["Alice", None, "Charlie", "David", None]}
        )

        result = checker.is_column_not_null(df, "name")

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_not_null"
        assert call_args[0][1] is False
        assert call_args[1]["null_count"] == 2

    # Tests for is_column_enum

    def test_is_column_enum_returns_true_when_all_values_valid(
        self, checker, mock_db_connector
    ):
        """Test that is_column_enum returns True when all values are in enum list"""
        df = pl.DataFrame(
            {"status": ["active", "inactive", "active", "pending", "inactive"]}
        )

        result = checker.is_column_enum(df, "status", ["active", "inactive", "pending"])

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_enum"
        assert call_args[0][1] is True

    def test_is_column_enum_returns_false_when_invalid_values_exist(
        self, checker, mock_db_connector
    ):
        """Test that is_column_enum returns False when values outside enum list exist"""
        df = pl.DataFrame(
            {"status": ["active", "inactive", "deleted", "pending", "archived"]}
        )

        result = checker.is_column_enum(df, "status", ["active", "inactive", "pending"])

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "is_column_enum"
        assert call_args[0][1] is False
        assert "deleted" in call_args[1]["invalid_values"]
        assert "archived" in call_args[1]["invalid_values"]

    def test_is_column_enum_ignores_null_values(self, checker, mock_db_connector):
        """Test that is_column_enum ignores null values in validation"""
        df = pl.DataFrame({"status": ["active", None, "inactive", "pending", None]})

        result = checker.is_column_enum(df, "status", ["active", "inactive", "pending"])

        assert result is True
        mock_db_connector.log.assert_called_once()

    # Tests for are_tables_referential_integral

    def test_are_tables_referential_integral_returns_true_when_all_references_exist(
        self, checker, mock_db_connector
    ):
        """Test that are_tables_referential_integral returns True when all foreign keys exist"""
        orders = pl.DataFrame(
            {"order_id": [1, 2, 3, 4], "customer_id": [101, 102, 103, 101]}
        )

        customers = pl.DataFrame(
            {
                "customer_id": [101, 102, 103, 104],
                "name": ["Alice", "Bob", "Charlie", "David"],
            }
        )

        result = checker.are_tables_referential_integral(
            orders, customers, ["customer_id"]
        )

        assert result is True
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "are_tables_referential_integral"
        assert call_args[0][1] is True
        assert call_args[1]["orphaned_rows"] == 0

    def test_are_tables_referential_integral_returns_false_when_orphaned_records_exist(
        self, checker, mock_db_connector
    ):
        """Test that are_tables_referential_integral returns False when orphaned records exist"""
        orders = pl.DataFrame(
            {"order_id": [1, 2, 3, 4], "customer_id": [101, 102, 999, 101]}
        )

        customers = pl.DataFrame(
            {
                "customer_id": [101, 102, 103, 104],
                "name": ["Alice", "Bob", "Charlie", "David"],
            }
        )

        result = checker.are_tables_referential_integral(
            orders, customers, ["customer_id"]
        )

        assert result is False
        mock_db_connector.log.assert_called_once()
        call_args = mock_db_connector.log.call_args
        assert call_args[0][0] == "are_tables_referential_integral"
        assert call_args[0][1] is False
        assert call_args[1]["orphaned_rows"] == 1

    def test_are_tables_referential_integral_with_composite_keys(
        self, checker, mock_db_connector
    ):
        """Test referential integrity with multiple join keys"""
        order_items = pl.DataFrame(
            {
                "order_id": [1, 1, 2, 2],
                "product_id": [10, 20, 10, 30],
                "quantity": [2, 1, 3, 1],
            }
        )

        products = pl.DataFrame(
            {
                "order_id": [1, 1, 2, 2],
                "product_id": [10, 20, 10, 30],
                "name": ["Widget", "Gadget", "Widget", "Gizmo"],
            }
        )

        result = checker.are_tables_referential_integral(
            order_items, products, ["order_id", "product_id"]
        )

        assert result is True
        mock_db_connector.log.assert_called_once()

    # Tests for log_results

    def test_log_results_calls_db_connector_log(self, checker, mock_db_connector):
        """Test that log_results properly delegates to db_connector.log"""
        checker.log_results(
            "is_column_unique", True, column="test_column", extra_info="test"
        )

        mock_db_connector.log.assert_called_once_with(
            "is_column_unique", True, column="test_column", extra_info="test"
        )
