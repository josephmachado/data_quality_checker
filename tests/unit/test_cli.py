from typer.testing import CliRunner
from data_quality_checker.cli import app
from unittest.mock import patch, MagicMock

runner = CliRunner()

@patch("data_quality_checker.cli.get_checker")
def test_check_unique(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_unique.return_value = True
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-unique", "--data", "users.csv", "--column", "user_id"])
    
    assert result.exit_code == 0
    assert "Column 'user_id' in 'users.csv' is unique" in result.stdout
    mock_checker.is_column_unique.assert_called_once_with("users.csv", "user_id")

@patch("data_quality_checker.cli.get_checker")
def test_check_unique_fail(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_unique.return_value = False
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-unique", "--data", "users.csv", "--column", "user_id"])
    
    assert result.exit_code == 0
    assert "Column 'user_id' in 'users.csv' is NOT unique" in result.stdout

@patch("data_quality_checker.cli.get_checker")
def test_check_not_null(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_not_null.return_value = True
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-not-null", "--data", "users.csv", "--column", "age"])
    
    assert result.exit_code == 0
    assert "Column 'age' in 'users.csv' has NO nulls" in result.stdout
    mock_checker.is_column_not_null.assert_called_once_with("users.csv", "age")

@patch("data_quality_checker.cli.get_checker")
def test_check_not_null_fail(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_not_null.return_value = False
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-not-null", "--data", "users.csv", "--column", "age"])
    
    assert result.exit_code == 0
    assert "Column 'age' in 'users.csv' HAS nulls" in result.stdout


@patch("data_quality_checker.cli.get_checker")
def test_check_enum(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_enum.return_value = True
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-enum", "--data", "users.csv", "--column", "status", "--enum-values", "active,inactive"])
    
    assert result.exit_code == 0
    assert "Column 'status' in 'users.csv' contains only allowed values" in result.stdout
    mock_checker.is_column_enum.assert_called_once_with("users.csv", "status", ["active", "inactive"])

@patch("data_quality_checker.cli.get_checker")
def test_check_enum_fail(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_enum.return_value = False
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-enum", "--data", "users.csv", "--column", "status", "--enum-values", "active,inactive"])
    
    assert result.exit_code == 0
    assert "Column 'status' in 'users.csv' contains invalid values" in result.stdout


@patch("data_quality_checker.cli.get_checker")
def test_check_references(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.are_tables_referential_integral.return_value = True
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-references", "--data", "orders.csv", "--reference", "users.csv", "--join-keys", "user_id"])
    
    assert result.exit_code == 0
    assert "Referential integrity maintained" in result.stdout
    mock_checker.are_tables_referential_integral.assert_called_once_with("orders.csv", "users.csv", ["user_id"])

@patch("data_quality_checker.cli.get_checker")
def test_check_references_fail(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.are_tables_referential_integral.return_value = False
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-references", "--data", "orders.csv", "--reference", "users.csv", "--join-keys", "user_id"])
    
    assert result.exit_code == 0
    assert "Referential integrity check FAILED" in result.stdout


@patch("data_quality_checker.cli.get_checker")
def test_check_column_exists(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_in_data.return_value = True
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-column-exists", "--data", "users.csv", "--column", "email"])
    
    assert result.exit_code == 0
    assert "Column 'email' exists in 'users.csv'" in result.stdout
    mock_checker.is_column_in_data.assert_called_once_with("users.csv", "email")

@patch("data_quality_checker.cli.get_checker")
def test_check_column_exists_fail(mock_get_checker):
    mock_checker = MagicMock()
    mock_checker.is_column_in_data.return_value = False
    mock_get_checker.return_value = mock_checker

    result = runner.invoke(app, ["check-column-exists", "--data", "users.csv", "--column", "email"])
    
    assert result.exit_code == 0
    assert "Column 'email' does NOT exist in 'users.csv'" in result.stdout


@patch("data_quality_checker.cli.DBConnector")
def test_show_logs(mock_db_connector):
    result = runner.invoke(app, ["show-logs"])
    assert result.exit_code == 0
    mock_db_connector.return_value.print_all_logs.assert_called_once()
