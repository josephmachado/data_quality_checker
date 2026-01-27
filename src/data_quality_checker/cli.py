from pathlib import Path
import typer
from rich.console import Console

from data_quality_checker.connector.output_log import DBConnector
from data_quality_checker.main import DataQualityChecker

app = typer.Typer(help="Data Quality Checker CLI")
console = Console()

def get_checker(db_path: str = "quality_checks.db") -> DataQualityChecker:
    """Helper to initialize DataQualityChecker with DBConnector."""
    db_connector = DBConnector(Path(db_path))  # pragma: no cover
    return DataQualityChecker(db_connector)  # pragma: no cover

@app.command()
def check_unique(
    data_path: str = typer.Option(..., "--data", help="Path to the data file"),
    column: str = typer.Option(..., "--column", help="Name of the column to check for uniqueness"),
    db_path: str = typer.Option("quality_checks.db", "--db-path", help="Path to the SQLite database for logging")
) -> None:
    """
    Check if a column contains unique values.
    """
    checker = get_checker(db_path)
    result = checker.is_column_unique(data_path, column)
    if result:
        console.print(f"[green]✓ Column '{column}' in '{data_path}' is unique.[/green]")
    else:
        console.print(f"[red]✗ Column '{column}' in '{data_path}' is NOT unique.[/red]")

@app.command()
def check_not_null(
    data_path: str = typer.Option(..., "--data", help="Path to the data file"),
    column: str = typer.Option(..., "--column", help="Name of the column to check for nulls"),
    db_path: str = typer.Option("quality_checks.db", "--db-path", help="Path to the SQLite database for logging")
) -> None:
    """
    Check if a column contains NO null values.
    """
    checker = get_checker(db_path)
    result = checker.is_column_not_null(data_path, column)
    if result:
        console.print(f"[green]✓ Column '{column}' in '{data_path}' has NO nulls.[/green]")
    else:
        console.print(f"[red]✗ Column '{column}' in '{data_path}' HAS nulls.[/red]")

@app.command()
def check_enum(
    data_path: str = typer.Option(..., "--data", help="Path to the data file"),
    column: str = typer.Option(..., "--column", help="Name of the column to check"),
    enum_values: str = typer.Option(..., "--enum-values", help="Allowed values for the column (comma-separated)"),
    db_path: str = typer.Option("quality_checks.db", "--db-path", help="Path to the SQLite database for logging")
) -> None:
    """
    Check if a column only contains values from a specified list.
    """
    values_list = [v.strip() for v in enum_values.split(",")]
    checker = get_checker(db_path)
    result = checker.is_column_enum(data_path, column, values_list)
    if result:
        console.print(f"[green]✓ Column '{column}' in '{data_path}' contains only allowed values.[/green]")
    else:
        console.print(f"[red]✗ Column '{column}' in '{data_path}' contains invalid values.[/red]")

@app.command()
def check_references(
    data_path: str = typer.Option(..., "--data", help="Path to the data file"),
    reference_path: str = typer.Option(..., "--reference", help="Path to the reference data file"),
    join_keys: str = typer.Option(..., "--join-keys", help="Column(s) to join on (comma-separated)"),
    db_path: str = typer.Option("quality_checks.db", "--db-path", help="Path to the SQLite database for logging")
) -> None:
    """
    Check referential integrity between two files.
    """
    keys_list = [k.strip() for k in join_keys.split(",")]
    checker = get_checker(db_path)
    result = checker.are_tables_referential_integral(data_path, reference_path, keys_list)
    if result:
        console.print(f"[green]✓ Referential integrity maintained between '{data_path}' and '{reference_path}'.[/green]")
    else:
        console.print(f"[red]✗ Referential integrity check FAILED.[/red]")

@app.command()
def check_column_exists(
    data_path: str = typer.Option(..., "--data", help="Path to the data file"),
    column: str = typer.Option(..., "--column", help="Name of the column to check"),
    db_path: str = typer.Option("quality_checks.db", "--db-path", help="Path to the SQLite database for logging")
) -> None:
    """
    Check if a column exists in the data file.
    """
    checker = get_checker(db_path)
    result = checker.is_column_in_data(data_path, column)
    if result:
        console.print(f"[green]✓ Column '{column}' exists in '{data_path}'.[/green]")
    else:
        console.print(f"[red]✗ Column '{column}' does NOT exist in '{data_path}'.[/red]")

@app.command()
def show_logs(
    db_path: str = typer.Option("quality_checks.db", "--db-path", help="Path to the SQLite database")
) -> None:
    """
    Show all validation logs from the database.
    """
    db_connector = DBConnector(Path(db_path))
    db_connector.print_all_logs()

if __name__ == "__main__":
    app()  # pragma: no cover
