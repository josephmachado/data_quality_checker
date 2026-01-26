import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Literal


class DBConnector:
    """Class to connect to a sqlite3 db
    and log data to a log table
    """

    def __init__(self, db_file: Path) -> None:
        """Function to initialize a sqlite3 db on the given db_file

        Args:
            db_file: The path to the db_file for sqlite3
        """
        self.db_file = db_file
        self._create_log_table()

    def _create_log_table(self) -> None:
        """Create the log table if it doesn't exist"""
        conn = sqlite3.connect(str(self.db_file))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                data_quality_check_type TEXT NOT NULL,
                result INTEGER NOT NULL,
                additional_params TEXT
            )
        """)
        conn.commit()
        conn.close()

    def log(
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
        """Function to load the results into a log table, with ts of when data is inserted

        Args:
            data_quality_check_type: Type of dq check that was run
            result: The result of the dq check
            **kwargs: Additional params to the dq check
        """
        timestamp = datetime.now().isoformat()
        additional_params = str(kwargs) if kwargs else None

        conn = sqlite3.connect(str(self.db_file))
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO log (timestamp, data_quality_check_type, result, additional_params)
            VALUES (?, ?, ?, ?)
        """,
            (timestamp, data_quality_check_type, int(result), additional_params),
        )
        conn.commit()
        conn.close()

    def print_all_logs(self) -> None:
        """Print all log entries in the table"""
        conn = sqlite3.connect(str(self.db_file))
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM log ORDER BY id")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            print("No log entries found.")
            return

        print(
            f"{'ID':<5} {'Timestamp':<26} {'Check Type':<35} {'Result':<8} {'Additional Params'}"
        )
        print("-" * 120)

        for row in rows:
            id_val, timestamp, check_type, result, params = row
            result_str = "PASS" if result else "FAIL"
            params_str = params if params else ""
            print(
                f"{id_val:<5} {timestamp:<26} {check_type:<35} {result_str:<8} {params_str}"
            )
