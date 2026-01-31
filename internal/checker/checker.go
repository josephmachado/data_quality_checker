package checker

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/josephmachado/data_quality_checker/internal/db"
	_ "github.com/marcboeker/go-duckdb"
)

type DataQualityChecker struct {
	dbConnector *db.DBConnector
}

// NewDataQualityChecker creates a new DataQualityChecker
func NewDataQualityChecker(dbConnector *db.DBConnector) *DataQualityChecker {
	return &DataQualityChecker{dbConnector: dbConnector}
}

// validatePathExists checks if file exists and is readable by DuckDB
func (c *DataQualityChecker) validatePathExists(dataPath string) error {
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		return fmt.Errorf("data path not found: %s", dataPath)
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// Check if DuckDB can parse header
	// Use string formatting for TABLE path as it's not always supported as bind param in FROM clause in all drivers/contexts
	query := fmt.Sprintf("SELECT * FROM '%s' LIMIT 0", dataPath)
	_, err = duckInfo.Exec(query)
	if err != nil {
		return fmt.Errorf("data path is not readable by DuckDB: %s. Error: %v", dataPath, err)
	}
	return nil
}

func (c *DataQualityChecker) IsColumnUnique(dataPath, uniqueColumn string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// SQL returns rows where duplicates exist (0 rows = success)
	// Query: SELECT uniqueColumn FROM 'dataPath' GROUP BY uniqueColumn HAVING COUNT(*) > 1
	subQuery := fmt.Sprintf("SELECT %s FROM '%s' GROUP BY %s HAVING COUNT(*) > 1", uniqueColumn, dataPath, uniqueColumn)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		// If query fails (e.g. column missing), we should probably return error
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      uniqueColumn,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_unique", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

func (c *DataQualityChecker) IsColumnNotNull(dataPath, notNullColumn string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	subQuery := fmt.Sprintf("SELECT * FROM '%s' WHERE %s IS NULL", dataPath, notNullColumn)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      notNullColumn,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_not_null", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

func (c *DataQualityChecker) IsColumnEnum(dataPath, enumColumn string, enumValues []string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// Format enum values as 'v1', 'v2'
	quotedValues := make([]string, len(enumValues))
	for i, v := range enumValues {
		quotedValues[i] = fmt.Sprintf("'%s'", v)
	}
	enumValsStr := strings.Join(quotedValues, ", ")

	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE %s NOT IN (%s) AND %s IS NOT NULL",
		enumColumn, dataPath, enumColumn, enumValsStr, enumColumn)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      enumColumn,
		"enum_values": enumValues,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_enum", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

func (c *DataQualityChecker) AreTablesReferentialIntegral(dataPath, referencePath string, joinKeys []string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}
	if err := c.validatePathExists(referencePath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	var joinConditionsParts []string
	var whereConditionsParts []string

	for _, key := range joinKeys {
		joinConditionsParts = append(joinConditionsParts, fmt.Sprintf("l.%s = r.%s", key, key))
		whereConditionsParts = append(whereConditionsParts, fmt.Sprintf("r.%s IS NULL", key))
	}

	joinConditions := strings.Join(joinConditionsParts, " AND ")
	whereConditions := strings.Join(whereConditionsParts, " AND ")

	subQuery := fmt.Sprintf(`
		SELECT l.* 
		FROM '%s' l 
		LEFT JOIN '%s' r ON %s 
		WHERE %s
	`, dataPath, referencePath, joinConditions, whereConditions)

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"join_keys":      joinKeys,
		"data_path":      dataPath,
		"reference_path": referencePath,
		"error_count":    errorCount,
	}
	if err := c.dbConnector.Log("are_tables_referential_integral", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

func (c *DataQualityChecker) IsColumnInData(dataPath, columnName string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		// Log failure as well? Python code didn't exist validation for this specific logic explicitly before call inside IsColumnInData,
		// but `is_column_in_data` in python:
		// 1. checked type
		// 2. validate_path_exists
		// 3. try select column limit 0
		// If exception (any), return False.

		// In Go, if path invalid, we return error conventionally.
		// However, to match Python exactly: "if not exists -> result = False".
		// But python code actually RAISES FileNotFoundError in `_validate_path_exists`.
		// `is_column_in_data` calls `self._validate_path_exists(data_path)`.
		// So if file is missing, it raises error.
		// If column is missing, duckdb select fails, catches exception, returns result=False.
		// So we should propagate error for path validation, but handle column missing as false result.
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	query := fmt.Sprintf("SELECT %s FROM '%s' LIMIT 0", columnName, dataPath)
	_, err = duckInfo.Exec(query)
	result := err == nil

	params := map[string]interface{}{
		"column":    columnName,
		"data_path": dataPath,
	}
	if err := c.dbConnector.Log("is_column_in_data", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}
