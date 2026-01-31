package db

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestNewDBConnector(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dqc_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	connector := NewDBConnector(dbPath)

	if connector == nil {
		t.Fatal("Expected connector to be non-nil")
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("Expected database file to exist at %s", dbPath)
	}

	// Verify table creation
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='log'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Error("Expected log table to exist")
	}
}

func TestLog(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dqc_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	connector := NewDBConnector(dbPath)

	// Test Log with true result
	params := map[string]interface{}{
		"column": "id",
	}
	err = connector.Log("test_check", true, params)
	if err != nil {
		t.Errorf("Failed to log: %v", err)
	}

	// Test Log with false result
	err = connector.Log("test_check_fail", false, nil)
	if err != nil {
		t.Errorf("Failed to log: %v", err)
	}

	// Verify contents
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT data_quality_check_type, result, additional_params FROM log ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// Check first entry
	if !rows.Next() {
		t.Fatal("Expected row 1")
	}
	var checkType string
	var result int
	var additionalParams sql.NullString
	if err := rows.Scan(&checkType, &result, &additionalParams); err != nil {
		t.Fatal(err)
	}
	if checkType != "test_check" {
		t.Errorf("Expected check type 'test_check', got %s", checkType)
	}
	if result != 1 {
		t.Errorf("Expected result 1, got %d", result)
	}
	if !additionalParams.Valid || additionalParams.String == "" {
		t.Error("Expected additional params")
	}

	// Check second entry
	if !rows.Next() {
		t.Fatal("Expected row 2")
	}
	if err := rows.Scan(&checkType, &result, &additionalParams); err != nil {
		t.Fatal(err)
	}
	if checkType != "test_check_fail" {
		t.Errorf("Expected check type 'test_check_fail', got %s", checkType)
	}
	if result != 0 {
		t.Errorf("Expected result 0, got %d", result)
	}
	if additionalParams.Valid { // It might be NULL or empty
		t.Logf("Additional params for empty map: %s", additionalParams.String)
	}
}

func TestLogTimestamp(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dqc_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	connector := NewDBConnector(dbPath)

	before := time.Now().Add(-1 * time.Second)
	connector.Log("check", true, nil)
	after := time.Now().Add(1 * time.Second)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var tsStr string
	err = db.QueryRow("SELECT timestamp FROM log").Scan(&tsStr)
	if err != nil {
		t.Fatal(err)
	}

	ts, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		// RFC3339 format check
		// The Go time.Format(time.RFC3339) includes offset.
		// Checking if it parses back is good enough.
		t.Errorf("Failed to parse timestamp %s: %v", tsStr, err)
	}

	if ts.Before(before) || ts.After(after) {
		t.Errorf("Timestamp %v out of expected range [%v, %v]", ts, before, after)
	}
}

func TestClearLogs(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "dqc_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	connector := NewDBConnector(dbPath)

	// Add some logs
	connector.Log("check1", true, nil)
	connector.Log("check2", false, nil)

	// Verify count is 2
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM log").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("Expected 2 logs, got %d", count)
	}
	db.Close()

	// Clear logs
	if err := connector.ClearLogs(); err != nil {
		t.Fatalf("Failed to clear logs: %v", err)
	}

	// Verify count is 0
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.QueryRow("SELECT COUNT(*) FROM log").Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("Expected 0 logs, got %d", count)
	}
}
