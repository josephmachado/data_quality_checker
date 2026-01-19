import sqlite3
from datetime import datetime

from data_quality_checker.connector.output_log import DBConnector


class TestDBConnector:
    def test_init_creates_database_file(self, temp_db_path):
        """Test that initializing DBConnector creates the database file"""
        assert not temp_db_path.exists()

        db_connector = DBConnector(temp_db_path)

        assert temp_db_path.exists()

    def test_init_creates_log_table(self, db_connector, temp_db_path):
        """Test that initializing DBConnector creates the log table"""
        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()

        # Query sqlite_master to check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='log'"
        )
        result = cursor.fetchone()
        conn.close()

        assert result is not None
        assert result[0] == "log"

    def test_log_table_has_correct_schema(self, db_connector, temp_db_path):
        """Test that the log table has the correct columns"""
        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(log)")
        columns = cursor.fetchall()
        conn.close()

        column_names = [col[1] for col in columns]
        expected_columns = [
            "id",
            "timestamp",
            "data_quality_check_type",
            "result",
            "additional_params",
        ]

        assert column_names == expected_columns

    def test_log_inserts_record_with_result_true(self, db_connector, temp_db_path):
        """Test that log method inserts a record with result=True"""
        db_connector.log(
            "is_column_unique", True, column="test_col", table="test_table"
        )

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM log")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 1
        assert rows[0][2] == "is_column_unique"
        assert rows[0][3] == 1  # True stored as 1
        assert "column" in rows[0][4]
        assert "test_col" in rows[0][4]

    def test_log_inserts_record_with_result_false(self, db_connector, temp_db_path):
        """Test that log method inserts a record with result=False"""
        db_connector.log("is_column_not_null", False, column="test_col", null_count=5)

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM log")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 1
        assert rows[0][2] == "is_column_not_null"
        assert rows[0][3] == 0  # False stored as 0
        assert "null_count" in rows[0][4]

    def test_log_inserts_multiple_records(self, db_connector, temp_db_path):
        """Test that multiple log calls insert multiple records"""
        db_connector.log("is_column_unique", True, column="col1")
        db_connector.log("is_column_not_null", False, column="col2")
        db_connector.log("is_column_enum", True, column="col3")

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM log")
        rows = cursor.fetchall()
        conn.close()

        assert len(rows) == 3
        assert rows[0][2] == "is_column_unique"
        assert rows[1][2] == "is_column_not_null"
        assert rows[2][2] == "is_column_enum"

    def test_log_stores_timestamp(self, db_connector, temp_db_path):
        """Test that log method stores a timestamp"""
        before_time = datetime.now()
        db_connector.log("is_column_unique", True, column="test_col")
        after_time = datetime.now()

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT timestamp FROM log")
        row = cursor.fetchone()
        conn.close()

        timestamp = datetime.fromisoformat(row[0])
        assert before_time <= timestamp <= after_time

    def test_log_stores_additional_params_as_string(self, db_connector, temp_db_path):
        """Test that additional params are stored as string"""
        db_connector.log(
            "is_column_enum",
            True,
            column="status",
            enum_values=["active", "inactive"],
            extra_info="test",
        )

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT additional_params FROM log")
        row = cursor.fetchone()
        conn.close()

        params = row[0]
        assert isinstance(params, str)
        assert "column" in params
        assert "status" in params
        assert "enum_values" in params

    def test_log_stores_none_when_no_additional_params(
        self, db_connector, temp_db_path
    ):
        """Test that additional_params is None when no kwargs provided"""
        db_connector.log("is_column_unique", True)

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT additional_params FROM log")
        row = cursor.fetchone()
        conn.close()

        assert row[0] is None

    def test_log_with_all_check_types(self, db_connector, temp_db_path):
        """Test logging all different check types"""
        check_types = [
            "is_column_unique",
            "is_column_not_null",
            "is_column_enum",
            "are_tables_referential_integral",
        ]

        for check_type in check_types:
            db_connector.log(check_type, True, column="test")

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT data_quality_check_type FROM log")
        rows = cursor.fetchall()
        conn.close()

        logged_types = [row[0] for row in rows]
        assert logged_types == check_types

    def test_print_all_logs_with_no_logs(self, db_connector, capsys):
        """Test print_all_logs when database is empty"""
        db_connector.print_all_logs()

        captured = capsys.readouterr()
        assert "No log entries found." in captured.out

    def test_print_all_logs_displays_records(self, db_connector, capsys):
        """Test print_all_logs displays all records"""
        db_connector.log("is_column_unique", True, column="col1")
        db_connector.log("is_column_not_null", False, column="col2")

        db_connector.print_all_logs()

        captured = capsys.readouterr()
        assert "is_column_unique" in captured.out
        assert "is_column_not_null" in captured.out
        assert "PASS" in captured.out
        assert "FAIL" in captured.out
        assert "col1" in captured.out
        assert "col2" in captured.out

    def test_print_all_logs_shows_correct_result_format(self, db_connector, capsys):
        """Test that print_all_logs shows PASS/FAIL correctly"""
        db_connector.log("is_column_unique", True, column="test")
        db_connector.log("is_column_not_null", False, column="test")

        db_connector.print_all_logs()

        captured = capsys.readouterr()
        lines = captured.out.split("\n")

        # Find lines with actual data (skip header and separator)
        data_lines = [
            line
            for line in lines
            if line and not line.startswith("ID") and not line.startswith("-")
        ]

        assert any("PASS" in line for line in data_lines)
        assert any("FAIL" in line for line in data_lines)

    def test_print_all_logs_orders_by_id(self, db_connector, capsys):
        """Test that print_all_logs displays records in order by id"""
        db_connector.log("is_column_unique", True, column="first")
        db_connector.log("is_column_not_null", False, column="second")
        db_connector.log("is_column_enum", True, column="third")

        db_connector.print_all_logs()

        captured = capsys.readouterr()

        # Check that "first" appears before "second" and "third"
        first_pos = captured.out.find("first")
        second_pos = captured.out.find("second")
        third_pos = captured.out.find("third")

        assert first_pos < second_pos < third_pos

    def test_multiple_instances_share_same_database(self, temp_db_path):
        """Test that multiple DBConnector instances can access the same database"""
        connector1 = DBConnector(temp_db_path)
        connector1.log("is_column_unique", True, column="test1")

        connector2 = DBConnector(temp_db_path)
        connector2.log("is_column_not_null", False, column="test2")

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM log")
        count = cursor.fetchone()[0]
        conn.close()

        assert count == 2

    def test_log_handles_special_characters_in_params(self, db_connector, temp_db_path):
        """Test that log handles special characters in additional params"""
        db_connector.log(
            "is_column_enum",
            True,
            column="test's column",
            values=["value with 'quotes'", 'value with "double quotes"'],
        )

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT additional_params FROM log")
        row = cursor.fetchone()
        conn.close()

        assert row[0] is not None
        assert "test's column" in row[0]

    def test_log_handles_empty_kwargs(self, db_connector, temp_db_path):
        """Test that log handles empty kwargs dict"""
        db_connector.log("is_column_unique", True, **{})

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT additional_params FROM log")
        row = cursor.fetchone()
        conn.close()

        assert row[0] is None

    def test_autoincrement_id_works(self, db_connector, temp_db_path):
        """Test that id field auto-increments correctly"""
        db_connector.log("is_column_unique", True)
        db_connector.log("is_column_not_null", False)
        db_connector.log("is_column_enum", True)

        conn = sqlite3.connect(str(temp_db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM log ORDER BY id")
        ids = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert ids == [1, 2, 3]
