package checker

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/josephmachado/data_quality_checker/internal/db"
	_ "github.com/marcboeker/go-duckdb"
)

// DataQualityChecker provides methods to perform various data quality checks
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

// IsColumnUnique checks if the specified column in the data file contains unique values.
// It returns true if all values are unique, false otherwise.
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

// IsColumnNotNull checks if the specified column in the data file contains any null values.
// It returns true if no null values are found, false otherwise.
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

// IsColumnEnum checks if the values in the specified column are within the allowed enum values.
// It returns true if all values are valid, false otherwise.
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

// AreTablesReferentialIntegral checks if the foreign key relationships between two tables are valid.
// It ensures that values in the joining columns of the data file exist in the reference file.
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

// IsColumnInData checks if the specified column exists in the data file.
// It returns true if the column exists, false otherwise.
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

// IsColumnBetween checks if the values in a column are within a numeric range [min, max].
func (c *DataQualityChecker) IsColumnBetween(dataPath, columnName string, min, max float64) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE %s < %f OR %s > %f", columnName, dataPath, columnName, min, columnName, max)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"min":         min,
		"max":         max,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnRegexMatch checks if string values in a column match a given RE2 regular expression.
func (c *DataQualityChecker) IsColumnRegexMatch(dataPath, columnName, regex string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// DuckDB uses regexp_matches(column, pattern) or column ~ pattern
	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE NOT (regexp_matches(%s, '%s')) AND %s IS NOT NULL",
		columnName, dataPath, columnName, regex, columnName)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"regex":       regex,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_regex_match", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnOfType checks if the values in a column can be cast to the specified DuckDB type.
func (c *DataQualityChecker) IsColumnOfType(dataPath, columnName, targetType string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// Try to cast and see if any nulls are produced where original wasn't null
	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE TRY_CAST(%s AS %s) IS NULL AND %s IS NOT NULL",
		columnName, dataPath, columnName, targetType, columnName)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"target_type": targetType,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_of_type", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnLengthBetween checks if the length of string or object values in a column is within [min, max].
func (c *DataQualityChecker) IsColumnLengthBetween(dataPath, columnName string, min, max int) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE length(%s) < %d OR length(%s) > %d",
		columnName, dataPath, columnName, min, columnName, max)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"min":         min,
		"max":         max,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_length_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnMaxBetween checks if the maximum value in a column is within [min, max].
func (c *DataQualityChecker) IsColumnMaxBetween(dataPath, columnName string, min, max float64) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	query := fmt.Sprintf("SELECT MAX(%s) FROM '%s'", columnName, dataPath)

	var maxValue float64
	err = duckInfo.QueryRow(query).Scan(&maxValue)
	if err != nil {
		return false, err
	}

	result := maxValue >= min && maxValue <= max

	params := map[string]interface{}{
		"column":      columnName,
		"max_value":   maxValue,
		"min_allowed": min,
		"max_allowed": max,
		"data_path":   dataPath,
	}
	if err := c.dbConnector.Log("is_column_max_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnMinBetween checks if the minimum value in a column is within [min, max].
func (c *DataQualityChecker) IsColumnMinBetween(dataPath, columnName string, min, max float64) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	query := fmt.Sprintf("SELECT MIN(%s) FROM '%s'", columnName, dataPath)

	var minValue float64
	err = duckInfo.QueryRow(query).Scan(&minValue)
	if err != nil {
		return false, err
	}

	result := minValue >= min && minValue <= max

	params := map[string]interface{}{
		"column":      columnName,
		"min_value":   minValue,
		"min_allowed": min,
		"max_allowed": max,
		"data_path":   dataPath,
	}
	if err := c.dbConnector.Log("is_column_min_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnMeanBetween checks if the mean value in a column is within [min, max].
func (c *DataQualityChecker) IsColumnMeanBetween(dataPath, columnName string, min, max float64) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	query := fmt.Sprintf("SELECT AVG(%s) FROM '%s'", columnName, dataPath)

	var avgValue float64
	err = duckInfo.QueryRow(query).Scan(&avgValue)
	if err != nil {
		return false, err
	}

	result := avgValue >= min && avgValue <= max

	params := map[string]interface{}{
		"column":      columnName,
		"avg_value":   avgValue,
		"min_allowed": min,
		"max_allowed": max,
		"data_path":   dataPath,
	}
	if err := c.dbConnector.Log("is_column_mean_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnMedianBetween checks if the median value in a column is within [min, max].
func (c *DataQualityChecker) IsColumnMedianBetween(dataPath, columnName string, min, max float64) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	query := fmt.Sprintf("SELECT MEDIAN(%s) FROM '%s'", columnName, dataPath)

	var medianValue float64
	err = duckInfo.QueryRow(query).Scan(&medianValue)
	if err != nil {
		return false, err
	}

	result := medianValue >= min && medianValue <= max

	params := map[string]interface{}{
		"column":       columnName,
		"median_value": medianValue,
		"min_allowed":  min,
		"max_allowed":  max,
		"data_path":    dataPath,
	}
	if err := c.dbConnector.Log("is_column_median_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnDateFormat checks if string values in a column match a given strftime date format.
func (c *DataQualityChecker) IsColumnDateFormat(dataPath, columnName, format string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// DuckDB strptime returns NULL if format doesn't match. Cast to VARCHAR to ensure it works even if auto-detected as DATE.
	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE strptime(CAST(%s AS VARCHAR), '%s') IS NULL AND %s IS NOT NULL",
		columnName, dataPath, columnName, format, columnName)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"format":      format,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_date_format", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsTableRowCountBetween checks if the total number of rows in the table is within [min, max].
func (c *DataQualityChecker) IsTableRowCountBetween(dataPath string, min, max int64) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	query := fmt.Sprintf("SELECT COUNT(*) FROM '%s'", dataPath)

	var rowCount int64
	err = duckInfo.QueryRow(query).Scan(&rowCount)
	if err != nil {
		return false, err
	}

	result := rowCount >= min && rowCount <= max

	params := map[string]interface{}{
		"row_count":   rowCount,
		"min_allowed": min,
		"max_allowed": max,
		"data_path":   dataPath,
	}
	if err := c.dbConnector.Log("is_table_row_count_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsTableColumnCountBetween checks if the number of columns in the table is within [min, max].
func (c *DataQualityChecker) IsTableColumnCountBetween(dataPath string, min, max int) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// DuckDB system view for columns
	// We need to be careful about table names. DuckDB treats file paths as table names in some contexts.
	query := fmt.Sprintf("SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM '%s')", dataPath)

	var colCount int
	err = duckInfo.QueryRow(query).Scan(&colCount)
	if err != nil {
		return false, err
	}

	result := colCount >= min && colCount <= max

	params := map[string]interface{}{
		"col_count":   colCount,
		"min_allowed": min,
		"max_allowed": max,
		"data_path":   dataPath,
	}
	if err := c.dbConnector.Log("is_table_column_count_between", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnNotInSet checks if values in a column are NOT present in a given "blacklisted" set.
func (c *DataQualityChecker) IsColumnNotInSet(dataPath, columnName string, blacklistedValues []string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	quotedValues := make([]string, len(blacklistedValues))
	for i, v := range blacklistedValues {
		quotedValues[i] = fmt.Sprintf("'%s'", v)
	}
	blackListStr := strings.Join(quotedValues, ", ")

	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE %s IN (%s)",
		columnName, dataPath, columnName, blackListStr)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"blacklist":   blacklistedValues,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_not_in_set", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnIncreasing checks if the values in a column are in strictly ascending order.
func (c *DataQualityChecker) IsColumnIncreasing(dataPath, columnName string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// Use window function LAG to compare with previous row
	subQuery := fmt.Sprintf(`
		SELECT %s, LAG(%s) OVER () as prev_val 
		FROM '%s'
	`, columnName, columnName, dataPath)

	errorQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) WHERE %s <= prev_val", subQuery, columnName)

	var errorCount int64
	err = duckInfo.QueryRow(errorQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_increasing", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// IsColumnDateParseable checks if values in a column can be parsed as dates by DuckDB.
func (c *DataQualityChecker) IsColumnDateParseable(dataPath, columnName string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	// TRY_CAST to DATE returns NULL if parsing fails
	subQuery := fmt.Sprintf("SELECT %s FROM '%s' WHERE TRY_CAST(%s AS DATE) IS NULL AND %s IS NOT NULL",
		columnName, dataPath, columnName, columnName)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("is_column_date_parseable", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// AreColumnPairsEqual checks if the values in two columns are equal for every row.
func (c *DataQualityChecker) AreColumnPairsEqual(dataPath, col1, col2 string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	subQuery := fmt.Sprintf("SELECT %s, %s FROM '%s' WHERE %s != %s OR (%s IS NULL AND %s IS NOT NULL) OR (%s IS NOT NULL AND %s IS NULL)",
		col1, col2, dataPath, col1, col2, col1, col2, col1, col2)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column1":     col1,
		"column2":     col2,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("are_column_pairs_equal", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}

// AreDistinctValuesInSet checks if all unique values in a column are within a predefined list.
func (c *DataQualityChecker) AreDistinctValuesInSet(dataPath, columnName string, allowedValues []string) (bool, error) {
	if err := c.validatePathExists(dataPath); err != nil {
		return false, err
	}

	duckInfo, err := sql.Open("duckdb", "")
	if err != nil {
		return false, fmt.Errorf("failed to open duckdb: %w", err)
	}
	defer duckInfo.Close()

	quotedValues := make([]string, len(allowedValues))
	for i, v := range allowedValues {
		quotedValues[i] = fmt.Sprintf("'%s'", v)
	}
	allowedStr := strings.Join(quotedValues, ", ")

	subQuery := fmt.Sprintf("SELECT DISTINCT %s FROM '%s' WHERE %s NOT IN (%s) AND %s IS NOT NULL",
		columnName, dataPath, columnName, allowedStr, columnName)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", subQuery)

	var errorCount int64
	err = duckInfo.QueryRow(countQuery).Scan(&errorCount)
	if err != nil {
		return false, err
	}

	result := errorCount == 0

	params := map[string]interface{}{
		"column":      columnName,
		"allowed":     allowedValues,
		"data_path":   dataPath,
		"error_count": errorCount,
	}
	if err := c.dbConnector.Log("are_distinct_values_in_set", result, params); err != nil {
		return result, fmt.Errorf("failed to log result: %w", err)
	}

	return result, nil
}
