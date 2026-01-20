import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")


@app.cell
def _():
    return


@app.cell
def _():
    from pathlib import Path

    import polars as pl

    from data_quality_checker.connector.output_log import DBConnector
    from data_quality_checker.main import DataQualityChecker

    return DBConnector, DataQualityChecker, Path, pl


@app.cell
def _(DBConnector, DataQualityChecker, Path, pl):
    # Initialize database connector
    db_connector = DBConnector(Path("quality_checks.db"))

    # Create data quality checker
    checker = DataQualityChecker(db_connector)

    # Sample DataFrames
    users_df = pl.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "charlie@example.com",
                "david@example.com",
                "eve@example.com",
            ],
            "status": ["active", "active", "inactive", "active", "pending"],
            "age": [25, 30, None, 35, 28],
        }
    )

    orders_df = pl.DataFrame(
        {
            "order_id": [101, 102, 103, 104],
            "user_id": [1, 2, 1, 3],
            "amount": [100.0, 250.0, 75.0, 150.0],
        }
    )

    # Run validation checks
    is_unique = checker.is_column_unique(users_df, "user_id")
    print(f"User ID unique: {is_unique}")

    no_nulls = checker.is_column_not_null(users_df, "age")
    print(f"Age has no nulls: {no_nulls}  ")

    valid_status = checker.is_column_enum(
        users_df, "status", ["active", "inactive", "pending"]
    )
    print(f"Status values valid: {valid_status}")

    referential_integrity = checker.are_tables_referential_integral(
        orders_df, users_df, ["user_id"]
    )
    print(f"Referential integrity: {referential_integrity}")

    # View all logged validation results
    print("\nValidation History:")
    db_connector.print_all_logs()
    return


@app.cell
def _():
    # let's break something
    some_dict = {"a": 1}
    some_dict["a"]
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
