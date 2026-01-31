package checker

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/josephmachado/data_quality_checker/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

func getTestDataPath(t *testing.T, filename string) string {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// checker directory is internal/checker
	// root is ../../
	// data is ../../tests/data
	path := filepath.Join(cwd, "..", "..", "tests", "data", filename)
	absPath, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	return absPath
}

func setup(t *testing.T) (*DataQualityChecker, string) {
	tempDir, err := os.MkdirTemp("", "dqc_checker_test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tempDir) })

	dbPath := filepath.Join(tempDir, "test.db")
	connector := db.NewDBConnector(dbPath)
	checker := NewDataQualityChecker(connector)

	return checker, dbPath
}

func TestDataQualityChecks(t *testing.T) {
	checker, _ := setup(t)

	t.Run("IsColumnUnique", func(t *testing.T) {
		// Pass
		path := getTestDataPath(t, "unique_data.csv")
		valid, err := checker.IsColumnUnique(path, "id")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !valid {
			t.Error("Expected unique_data.csv to be unique")
		}

		// Fail
		path = getTestDataPath(t, "duplicate_data.csv")
		valid, err = checker.IsColumnUnique(path, "id")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if valid {
			t.Error("Expected duplicate_data.csv to NOT be unique")
		}
	})

	t.Run("IsColumnNotNull", func(t *testing.T) {
		// Pass
		path := getTestDataPath(t, "no_nulls.csv")
		valid, err := checker.IsColumnNotNull(path, "name")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !valid {
			t.Error("Expected no_nulls.csv to have no nulls")
		}

		// Fail
		path = getTestDataPath(t, "has_nulls.csv")
		valid, err = checker.IsColumnNotNull(path, "name")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if valid {
			t.Error("Expected has_nulls.csv to have nulls")
		}
	})

	t.Run("IsColumnEnum", func(t *testing.T) {
		enumValues := []string{"active", "inactive", "pending"}

		// Pass
		path := getTestDataPath(t, "valid_enum.csv")
		valid, err := checker.IsColumnEnum(path, "status", enumValues)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !valid {
			t.Error("Expected valid_enum.csv to pass enum check")
		}

		// Fail
		path = getTestDataPath(t, "invalid_enum.csv")
		valid, err = checker.IsColumnEnum(path, "status", enumValues)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if valid {
			t.Error("Expected invalid_enum.csv to fail enum check")
		}
	})

	t.Run("AreTablesReferentialIntegral", func(t *testing.T) {
		joinKeys := []string{"user_id"}
		usersPath := getTestDataPath(t, "users.csv")

		// Pass - orders.csv has valid user_ids
		ordersPath := getTestDataPath(t, "orders.csv")
		valid, err := checker.AreTablesReferentialIntegral(ordersPath, usersPath, joinKeys)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !valid {
			t.Error("Expected referential integrity to pass")
		}

		// Fail - orphaned_orders.csv has user_id not in users.csv
		orphanedPath := getTestDataPath(t, "orphaned_orders.csv")
		valid, err = checker.AreTablesReferentialIntegral(orphanedPath, usersPath, joinKeys)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if valid {
			t.Error("Expected referential integrity to fail")
		}
	})

	t.Run("IsColumnInData", func(t *testing.T) {
		path := getTestDataPath(t, "users.csv")

		// Pass
		valid, err := checker.IsColumnInData(path, "user_id")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !valid {
			t.Error("Expected user_id to exist in users.csv")
		}

		// Fail
		valid, err = checker.IsColumnInData(path, "non_existent_col")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if valid {
			t.Error("Expected non_existent_col to NOT exist")
		}
	})

	t.Run("IsColumnBetween", func(t *testing.T) {
		path := writeTempCSV(t, "age\n20\n30\n40")

		valid, _ := checker.IsColumnBetween(path, "age", 18, 50)
		if !valid {
			t.Error("Expected true for [18, 50]")
		}

		valid, _ = checker.IsColumnBetween(path, "age", 25, 50)
		if valid {
			t.Error("Expected false for [25, 50]")
		}
	})

	t.Run("IsColumnRegexMatch", func(t *testing.T) {
		path := writeTempCSV(t, "email\na@b.com\nc@d.com")

		valid, _ := checker.IsColumnRegexMatch(path, "email", `^[a-z]+@[a-z]+\.com$`)
		if !valid {
			t.Error("Expected true for matching regex")
		}

		valid, _ = checker.IsColumnRegexMatch(path, "email", `^[0-9]+$`)
		if valid {
			t.Error("Expected false for non-matching regex")
		}
	})

	t.Run("IsColumnOfType", func(t *testing.T) {
		path := writeTempCSV(t, "val\n1\n2\n3")
		valid, _ := checker.IsColumnOfType(path, "val", "INTEGER")
		if !valid {
			t.Error("Expected true for INTEGER")
		}

		path = writeTempCSV(t, "val\n1\n2\nabc")
		valid, _ = checker.IsColumnOfType(path, "val", "INTEGER")
		if valid {
			t.Error("Expected false for invalid INTEGER")
		}
	})

	t.Run("IsColumnLengthBetween", func(t *testing.T) {
		path := writeTempCSV(t, "name\nAlice\nBob")
		valid, _ := checker.IsColumnLengthBetween(path, "name", 3, 5)
		if !valid {
			t.Error("Expected true for [3, 5]")
		}

		valid, _ = checker.IsColumnLengthBetween(path, "name", 4, 5)
		if valid {
			t.Error("Expected false for [4, 5]")
		}
	})

	t.Run("AggregateChecks", func(t *testing.T) {
		path := writeTempCSV(t, "val\n10\n20\n30")

		v, _ := checker.IsColumnMaxBetween(path, "val", 25, 35)
		if !v {
			t.Error("Max failed")
		}
		v, _ = checker.IsColumnMinBetween(path, "val", 5, 15)
		if !v {
			t.Error("Min failed")
		}
		v, _ = checker.IsColumnMeanBetween(path, "val", 15, 25)
		if !v {
			t.Error("Mean failed")
		}
		v, _ = checker.IsColumnMedianBetween(path, "val", 15, 25)
		if !v {
			t.Error("Median failed")
		}
	})

	t.Run("TableLevelChecks", func(t *testing.T) {
		path := writeTempCSV(t, "a,b,c\n1,2,3\n4,5,6")

		v, _ := checker.IsTableRowCountBetween(path, 1, 3)
		if !v {
			t.Error("Row count failed")
		}
		v, _ = checker.IsTableColumnCountBetween(path, 2, 4)
		if !v {
			t.Error("Col count failed")
		}
	})

	t.Run("SetMembership", func(t *testing.T) {
		path := writeTempCSV(t, "color\nred\nblue\ngreen")

		v, _ := checker.IsColumnNotInSet(path, "color", []string{"yellow", "black"})
		if !v {
			t.Error("NotInSet failed")
		}
		v, _ = checker.IsColumnNotInSet(path, "color", []string{"red"})
		if v {
			t.Error("NotInSet should have failed")
		}

		v, _ = checker.AreDistinctValuesInSet(path, "color", []string{"red", "blue", "green", "yellow"})
		if !v {
			t.Error("DistinctInSet failed")
		}
	})

	t.Run("OrderingAndDate", func(t *testing.T) {
		path := writeTempCSV(t, "val\n1\n2\n3")
		v, _ := checker.IsColumnIncreasing(path, "val")
		if !v {
			t.Error("Increasing failed")
		}

		path = writeTempCSV(t, "dt\n2023-01-01\n2023-05-01")
		v, _ = checker.IsColumnDateParseable(path, "dt")
		if !v {
			t.Error("Date parseable failed")
		}
		v, _ = checker.IsColumnDateFormat(path, "dt", "%Y-%m-%d")
		if !v {
			t.Error("Date format failed")
		}
	})

	t.Run("ColumnPairEqual", func(t *testing.T) {
		path := writeTempCSV(t, "a,b\n1,1\n2,2")
		v, _ := checker.AreColumnPairsEqual(path, "a", "b")
		if !v {
			t.Error("Pair equal failed")
		}

		path = writeTempCSV(t, "a,b\n1,1\n2,3")
		v, _ = checker.AreColumnPairsEqual(path, "a", "b")
		if v {
			t.Error("Pair equal should have failed")
		}
	})
}

func TestLogsAreWritten(t *testing.T) {
	checker, dbPath := setup(t)
	path := getTestDataPath(t, "unique_data.csv")

	_, err := checker.IsColumnUnique(path, "id")
	if err != nil {
		t.Fatal(err)
	}

	// Connect to DB and check logs
	sqliteDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteDB.Close()

	var count int
	err = sqliteDB.QueryRow("SELECT COUNT(*) FROM log").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("Expected 1 log entry, got %d", count)
	}
}

func writeTempCSV(t *testing.T, content string) string {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "test.csv")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
