package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// DBConnector handles interactions with the SQLite database
type DBConnector struct {
	dbPath string
}

// LogEntry represents a row in the log table
type LogEntry struct {
	ID                   int
	Timestamp            string
	DataQualityCheckType string
	Result               bool
	AdditionalParams     string
}

// NewDBConnector creates a new DBConnector
func NewDBConnector(dbPath string) *DBConnector {
	absPath, err := filepath.Abs(dbPath)
	if err != nil {
		// Fallback to original path if absolute path fails, though rarely happens
		absPath = dbPath
	}
	connector := &DBConnector{dbPath: absPath}
	if err := connector.createLogTable(); err != nil {
		log.Printf("Warning: Failed to create log table: %v", err)
	}
	return connector
}

func (c *DBConnector) createLogTable() error {
	db, err := sql.Open("sqlite3", c.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	defer db.Close()

	query := `
	CREATE TABLE IF NOT EXISTS log (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp TEXT NOT NULL,
		data_quality_check_type TEXT NOT NULL,
		result INTEGER NOT NULL,
		additional_params TEXT
	)`

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	return nil
}

// Log inserts a new record into the log table
func (c *DBConnector) Log(checkType string, result bool, params map[string]interface{}) error {
	db, err := sql.Open("sqlite3", c.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	defer db.Close()

	timestamp := time.Now().Format(time.RFC3339)
	resultInt := 0
	if result {
		resultInt = 1
	}

	var additionalParams *string
	if len(params) > 0 {
		// Python code stored the string representation of the dict.
		// We can store it as JSON for better structure, or string representation to check parity.
		// The python code used `str(kwargs)`.
		// Using JSON in Go is cleaner.
		paramsBytes, err := json.Marshal(params)
		if err != nil {
			// Fallback to simpler string repr if marshal fails (unlikely for basic types)
			s := fmt.Sprintf("%v", params)
			additionalParams = &s
		} else {
			s := string(paramsBytes)
			additionalParams = &s
		}
	}

	query := `
	INSERT INTO log (timestamp, data_quality_check_type, result, additional_params)
	VALUES (?, ?, ?, ?)
	`
	_, err = db.Exec(query, timestamp, checkType, resultInt, additionalParams)
	if err != nil {
		return fmt.Errorf("failed to insert log: %w", err)
	}
	return nil
}

// PrintAllLogs prints all logs to stdout
func (c *DBConnector) PrintAllLogs() error {
	db, err := sql.Open("sqlite3", c.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	defer db.Close()

	query := "SELECT id, timestamp, data_quality_check_type, result, additional_params FROM log ORDER BY id"
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	var entries []LogEntry
	for rows.Next() {
		var e LogEntry
		var resultInt int
		var additionalParams sql.NullString
		if err := rows.Scan(&e.ID, &e.Timestamp, &e.DataQualityCheckType, &resultInt, &additionalParams); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		e.Result = resultInt != 0
		if additionalParams.Valid {
			e.AdditionalParams = additionalParams.String
		}
		entries = append(entries, e)
	}

	if len(entries) == 0 {
		fmt.Println("No log entries found.")
		return nil
	}

	// Format matching Python output
	// Python: f"{'ID':<5} {'Timestamp':<26} {'Check Type':<35} {'Result':<8} {'Additional Params'}"
	fmt.Printf("%-5s %-26s %-35s %-8s %s\n", "ID", "Timestamp", "Check Type", "Result", "Additional Params")
	fmt.Println("------------------------------------------------------------------------------------------------------------------------")

	for _, e := range entries {
		resStr := "FAIL"
		if e.Result {
			resStr = "PASS"
		}
		fmt.Printf("%-5d %-26s %-35s %-8s %s\n", e.ID, e.Timestamp, e.DataQualityCheckType, resStr, e.AdditionalParams)
	}

	return nil
}

// ClearLogs removes all entries from the log table
func (c *DBConnector) ClearLogs() error {
	db, err := sql.Open("sqlite3", c.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec("DELETE FROM log"); err != nil {
		return fmt.Errorf("failed to clear logs: %w", err)
	}

	return nil
}
