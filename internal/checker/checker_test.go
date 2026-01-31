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

func TestIsColumnUnique(t *testing.T) {
	checker, _ := setup(t)

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
}

func TestIsColumnNotNull(t *testing.T) {
	checker, _ := setup(t)

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
}

func TestIsColumnEnum(t *testing.T) {
	checker, _ := setup(t)
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
}

func TestAreTablesReferentialIntegral(t *testing.T) {
	checker, _ := setup(t)
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
}

func TestIsColumnInData(t *testing.T) {
	checker, _ := setup(t)
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
