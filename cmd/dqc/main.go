package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/josephmachado/data_quality_checker/internal/checker"
	"github.com/josephmachado/data_quality_checker/internal/db"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

var (
	dbPath string
)

// main is the entry point for the Data Quality Checker CLI application
func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "dqc",
	Short: "Data Quality Checker CLI",
	Long:  `A CLI tool for validating data quality on CSV/Parquet files using DuckDB.`,
}

func init() {
	// Persistent flag for DB path, as it's common to all commands (conceptually)
	// In Python it was repeated for each command.
	rootCmd.PersistentFlags().StringVar(&dbPath, "db-path", "quality_checks.db", "Path to the SQLite database for logging")

	rootCmd.AddCommand(checkUniqueCmd)
	rootCmd.AddCommand(checkNotNullCmd)
	rootCmd.AddCommand(checkEnumCmd)
	rootCmd.AddCommand(checkReferencesCmd)
	rootCmd.AddCommand(checkColumnExistsCmd)
	rootCmd.AddCommand(checkBetweenCmd)
	rootCmd.AddCommand(checkRegexCmd)
	rootCmd.AddCommand(checkTypeCmd)
	rootCmd.AddCommand(checkLengthCmd)
	rootCmd.AddCommand(checkMaxCmd)
	rootCmd.AddCommand(checkMinCmd)
	rootCmd.AddCommand(checkMeanCmd)
	rootCmd.AddCommand(checkMedianCmd)
	rootCmd.AddCommand(checkDateFormatCmd)
	rootCmd.AddCommand(checkRowCountCmd)
	rootCmd.AddCommand(checkColCountCmd)
	rootCmd.AddCommand(checkNotInSetCmd)
	rootCmd.AddCommand(checkIncreasingCmd)
	rootCmd.AddCommand(checkDateParseableCmd)
	rootCmd.AddCommand(checkPairEqualCmd)
	rootCmd.AddCommand(checkDistinctInSetCmd)
	rootCmd.AddCommand(showLogsCmd)
	rootCmd.AddCommand(cleanLogsCmd)
}

// getChecker initializes a new DataQualityChecker with the configured database path
func getChecker() *checker.DataQualityChecker {
	connector := db.NewDBConnector(dbPath)
	return checker.NewDataQualityChecker(connector)
}

var checkUniqueCmd = &cobra.Command{
	Use:   "check-unique",
	Short: "Check if a column contains unique values",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnUnique(dataPath, column)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' is unique.\n", column, dataPath)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' is NOT unique.\n", column, dataPath)
		}
	},
}

var checkNotNullCmd = &cobra.Command{
	Use:   "check-not-null",
	Short: "Check if a column contains NO null values",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnNotNull(dataPath, column)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' has NO nulls.\n", column, dataPath)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' HAS nulls.\n", column, dataPath)
		}
	},
}

var checkEnumCmd = &cobra.Command{
	Use:   "check-enum",
	Short: "Check if a column only contains values from a specified list",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		enumValuesStr, _ := cmd.Flags().GetString("enum-values")

		if dataPath == "" || column == "" || enumValuesStr == "" {
			pterm.Error.Println("Missing required flags: --data, --column, --enum-values")
			return
		}

		enumValues := strings.Split(enumValuesStr, ",")
		for i := range enumValues {
			enumValues[i] = strings.TrimSpace(enumValues[i])
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnEnum(dataPath, column, enumValues)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' contains only allowed values.\n", column, dataPath)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' contains invalid values.\n", column, dataPath)
		}
	},
}

var checkReferencesCmd = &cobra.Command{
	Use:   "check-references",
	Short: "Check referential integrity between two files",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		refPath, _ := cmd.Flags().GetString("reference")
		joinKeysStr, _ := cmd.Flags().GetString("join-keys")

		if dataPath == "" || refPath == "" || joinKeysStr == "" {
			pterm.Error.Println("Missing required flags: --data, --reference, --join-keys")
			return
		}

		joinKeys := strings.Split(joinKeysStr, ",")
		for i := range joinKeys {
			joinKeys[i] = strings.TrimSpace(joinKeys[i])
		}

		dqChecker := getChecker()
		valid, err := dqChecker.AreTablesReferentialIntegral(dataPath, refPath, joinKeys)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Referential integrity maintained between '%s' and '%s'.\n", dataPath, refPath)
		} else {
			pterm.Error.Println("Referential integrity check FAILED.")
		}
	},
}

var checkColumnExistsCmd = &cobra.Command{
	Use:   "check-column-exists",
	Short: "Check if a column exists in the data file",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnInData(dataPath, column)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' exists in '%s'.\n", column, dataPath)
		} else {
			pterm.Error.Printf("Column '%s' does NOT exist in '%s'.\n", column, dataPath)
		}
	},
}

var checkBetweenCmd = &cobra.Command{
	Use:   "check-between",
	Short: "Check if column values are within a numeric range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		min, _ := cmd.Flags().GetFloat64("min")
		max, _ := cmd.Flags().GetFloat64("max")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnBetween(dataPath, column, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' is within range [%v, %v].\n", column, dataPath, min, max)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' has values OUTSIDE range [%v, %v].\n", column, dataPath, min, max)
		}
	},
}

var checkRegexCmd = &cobra.Command{
	Use:   "check-regex",
	Short: "Check if column values match a regex",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		regex, _ := cmd.Flags().GetString("regex")

		if dataPath == "" || column == "" || regex == "" {
			pterm.Error.Println("Missing required flags: --data, --column, and --regex")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnRegexMatch(dataPath, column, regex)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' matches regex '%s'.\n", column, dataPath, regex)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' does NOT match regex '%s'.\n", column, dataPath, regex)
		}
	},
}

var checkTypeCmd = &cobra.Command{
	Use:   "check-type",
	Short: "Check if column values match a DuckDB type",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		targetType, _ := cmd.Flags().GetString("type")

		if dataPath == "" || column == "" || targetType == "" {
			pterm.Error.Println("Missing required flags: --data, --column, and --type")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnOfType(dataPath, column, targetType)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' matches type '%s'.\n", column, dataPath, targetType)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' does NOT match type '%s'.\n", column, dataPath, targetType)
		}
	},
}

var checkLengthCmd = &cobra.Command{
	Use:   "check-length",
	Short: "Check if column value lengths are within range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		min, _ := cmd.Flags().GetInt("min")
		max, _ := cmd.Flags().GetInt("max")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnLengthBetween(dataPath, column, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' length in '%s' is within [%d, %d].\n", column, dataPath, min, max)
		} else {
			pterm.Error.Printf("Column '%s' length in '%s' is OUTSIDE [%d, %d].\n", column, dataPath, min, max)
		}
	},
}

var checkMaxCmd = &cobra.Command{
	Use:   "check-max",
	Short: "Check if the maximum value in a column is within range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		min, _ := cmd.Flags().GetFloat64("min")
		max, _ := cmd.Flags().GetFloat64("max")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnMaxBetween(dataPath, column, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' max in '%s' is within [%v, %v].\n", column, dataPath, min, max)
		} else {
			pterm.Error.Printf("Column '%s' max in '%s' is OUTSIDE [%v, %v].\n", column, dataPath, min, max)
		}
	},
}

var checkMinCmd = &cobra.Command{
	Use:   "check-min",
	Short: "Check if the minimum value in a column is within range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		min, _ := cmd.Flags().GetFloat64("min")
		max, _ := cmd.Flags().GetFloat64("max")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnMinBetween(dataPath, column, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' min in '%s' is within [%v, %v].\n", column, dataPath, min, max)
		} else {
			pterm.Error.Printf("Column '%s' min in '%s' is OUTSIDE [%v, %v].\n", column, dataPath, min, max)
		}
	},
}

var checkMeanCmd = &cobra.Command{
	Use:   "check-mean",
	Short: "Check if the mean value in a column is within range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		min, _ := cmd.Flags().GetFloat64("min")
		max, _ := cmd.Flags().GetFloat64("max")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnMeanBetween(dataPath, column, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' mean in '%s' is within [%v, %v].\n", column, dataPath, min, max)
		} else {
			pterm.Error.Printf("Column '%s' mean in '%s' is OUTSIDE [%v, %v].\n", column, dataPath, min, max)
		}
	},
}

var checkMedianCmd = &cobra.Command{
	Use:   "check-median",
	Short: "Check if the median value in a column is within range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		min, _ := cmd.Flags().GetFloat64("min")
		max, _ := cmd.Flags().GetFloat64("max")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnMedianBetween(dataPath, column, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' median in '%s' is within [%v, %v].\n", column, dataPath, min, max)
		} else {
			pterm.Error.Printf("Column '%s' median in '%s' is OUTSIDE [%v, %v].\n", column, dataPath, min, max)
		}
	},
}

var checkDateFormatCmd = &cobra.Command{
	Use:   "check-date-format",
	Short: "Check if column values match a date format",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		format, _ := cmd.Flags().GetString("format")

		if dataPath == "" || column == "" || format == "" {
			pterm.Error.Println("Missing required flags: --data, --column, and --format")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnDateFormat(dataPath, column, format)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' matches format '%s'.\n", column, dataPath, format)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' does NOT match format '%s'.\n", column, dataPath, format)
		}
	},
}

var checkRowCountCmd = &cobra.Command{
	Use:   "check-row-count",
	Short: "Check if the table row count is within range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		min, _ := cmd.Flags().GetInt64("min")
		max, _ := cmd.Flags().GetInt64("max")

		if dataPath == "" {
			pterm.Error.Println("Missing required flag: --data")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsTableRowCountBetween(dataPath, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Table '%s' row count is within [%d, %d].\n", dataPath, min, max)
		} else {
			pterm.Error.Printf("Table '%s' row count is OUTSIDE [%d, %d].\n", dataPath, min, max)
		}
	},
}

var checkColCountCmd = &cobra.Command{
	Use:   "check-col-count",
	Short: "Check if the table column count is within range",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		min, _ := cmd.Flags().GetInt("min")
		max, _ := cmd.Flags().GetInt("max")

		if dataPath == "" {
			pterm.Error.Println("Missing required flag: --data")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsTableColumnCountBetween(dataPath, min, max)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Table '%s' column count is within [%d, %d].\n", dataPath, min, max)
		} else {
			pterm.Error.Printf("Table '%s' column count is OUTSIDE [%d, %d].\n", dataPath, min, max)
		}
	},
}

var checkNotInSetCmd = &cobra.Command{
	Use:   "check-not-in-set",
	Short: "Check if column values are NOT in a specified list",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		valuesStr, _ := cmd.Flags().GetString("values")

		if dataPath == "" || column == "" || valuesStr == "" {
			pterm.Error.Println("Missing required flags: --data, --column, and --values")
			return
		}

		values := strings.Split(valuesStr, ",")
		for i := range values {
			values[i] = strings.TrimSpace(values[i])
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnNotInSet(dataPath, column, values)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' contains NO values from the blacklist.\n", column, dataPath)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' HAS values from the blacklist.\n", column, dataPath)
		}
	},
}

var checkIncreasingCmd = &cobra.Command{
	Use:   "check-increasing",
	Short: "Check if column values are in strictly increasing order",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnIncreasing(dataPath, column)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' is strictly increasing.\n", column, dataPath)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' is NOT strictly increasing.\n", column, dataPath)
		}
	},
}

var checkDateParseableCmd = &cobra.Command{
	Use:   "check-date-parseable",
	Short: "Check if column values are parseable as dates",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")

		if dataPath == "" || column == "" {
			pterm.Error.Println("Missing required flags: --data and --column")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.IsColumnDateParseable(dataPath, column)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Column '%s' in '%s' is date-parseable.\n", column, dataPath)
		} else {
			pterm.Error.Printf("Column '%s' in '%s' is NOT date-parseable.\n", column, dataPath)
		}
	},
}

var checkPairEqualCmd = &cobra.Command{
	Use:   "check-pair-equal",
	Short: "Check if two columns have equal values in every row",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		col1, _ := cmd.Flags().GetString("col1")
		col2, _ := cmd.Flags().GetString("col2")

		if dataPath == "" || col1 == "" || col2 == "" {
			pterm.Error.Println("Missing required flags: --data, --col1, and --col2")
			return
		}

		dqChecker := getChecker()
		valid, err := dqChecker.AreColumnPairsEqual(dataPath, col1, col2)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("Columns '%s' and '%s' in '%s' are equal in every row.\n", col1, col2, dataPath)
		} else {
			pterm.Error.Printf("Columns '%s' and '%s' in '%s' are NOT equal in every row.\n", col1, col2, dataPath)
		}
	},
}

var checkDistinctInSetCmd = &cobra.Command{
	Use:   "check-distinct-in-set",
	Short: "Check if all unique values in a column are in a specified list",
	Run: func(cmd *cobra.Command, args []string) {
		dataPath, _ := cmd.Flags().GetString("data")
		column, _ := cmd.Flags().GetString("column")
		valuesStr, _ := cmd.Flags().GetString("values")

		if dataPath == "" || column == "" || valuesStr == "" {
			pterm.Error.Println("Missing required flags: --data, --column, and --values")
			return
		}

		values := strings.Split(valuesStr, ",")
		for i := range values {
			values[i] = strings.TrimSpace(values[i])
		}

		dqChecker := getChecker()
		valid, err := dqChecker.AreDistinctValuesInSet(dataPath, column, values)
		if err != nil {
			pterm.Error.Printf("Error: %v\n", err)
			return
		}

		if valid {
			pterm.Success.Printf("All unique values in column '%s' are within the allowed set.\n", column)
		} else {
			pterm.Error.Printf("Column '%s' has unique values OUTSIDE the allowed set.\n", column)
		}
	},
}

var showLogsCmd = &cobra.Command{
	Use:   "show-logs",
	Short: "Show all validation logs from the database",
	Run: func(cmd *cobra.Command, args []string) {
		connector := db.NewDBConnector(dbPath)
		if err := connector.PrintAllLogs(); err != nil {
			pterm.Error.Printf("Error printing logs: %v\n", err)
		}
	},
}

var cleanLogsCmd = &cobra.Command{
	Use:   "clean-logs",
	Short: "Clear all validation logs from the database",
	Run: func(cmd *cobra.Command, args []string) {
		connector := db.NewDBConnector(dbPath)
		if err := connector.ClearLogs(); err != nil {
			pterm.Error.Printf("Error clearing logs: %v\n", err)
			return
		}
		pterm.Success.Println("Logs cleared successfully.")
	},
}

func init() {
	checkUniqueCmd.Flags().String("data", "", "Path to the data file")
	checkUniqueCmd.Flags().String("column", "", "Name of the column to check")

	checkNotNullCmd.Flags().String("data", "", "Path to the data file")
	checkNotNullCmd.Flags().String("column", "", "Name of the column to check")

	checkEnumCmd.Flags().String("data", "", "Path to the data file")
	checkEnumCmd.Flags().String("column", "", "Name of the column to check")
	checkEnumCmd.Flags().String("enum-values", "", "Allowed values (comma-separated)")

	checkReferencesCmd.Flags().String("data", "", "Path to the data file")
	checkReferencesCmd.Flags().String("reference", "", "Path to the reference data file")
	checkReferencesCmd.Flags().String("join-keys", "", "Column(s) to join on (comma-separated)")

	checkColumnExistsCmd.Flags().String("data", "", "Path to the data file")
	checkColumnExistsCmd.Flags().String("column", "", "Name of the column to check")

	checkBetweenCmd.Flags().String("data", "", "Path to the data file")
	checkBetweenCmd.Flags().String("column", "", "Name of the column to check")
	checkBetweenCmd.Flags().Float64("min", 0, "Minimum value")
	checkBetweenCmd.Flags().Float64("max", 0, "Maximum value")

	checkRegexCmd.Flags().String("data", "", "Path to the data file")
	checkRegexCmd.Flags().String("column", "", "Name of the column to check")
	checkRegexCmd.Flags().String("regex", "", "Regex pattern to match")

	checkTypeCmd.Flags().String("data", "", "Path to the data file")
	checkTypeCmd.Flags().String("column", "", "Name of the column to check")
	checkTypeCmd.Flags().String("type", "", "DuckDB type (e.g., INTEGER, VARCHAR, DATE)")

	checkLengthCmd.Flags().String("data", "", "Path to the data file")
	checkLengthCmd.Flags().String("column", "", "Name of the column to check")
	checkLengthCmd.Flags().Int("min", 0, "Minimum length")
	checkLengthCmd.Flags().Int("max", 0, "Maximum length")

	checkMaxCmd.Flags().String("data", "", "Path to the data file")
	checkMaxCmd.Flags().String("column", "", "Name of the column to check")
	checkMaxCmd.Flags().Float64("min", 0, "Minimum allowed max value")
	checkMaxCmd.Flags().Float64("max", 0, "Maximum allowed max value")

	checkMinCmd.Flags().String("data", "", "Path to the data file")
	checkMinCmd.Flags().String("column", "", "Name of the column to check")
	checkMinCmd.Flags().Float64("min", 0, "Minimum allowed min value")
	checkMinCmd.Flags().Float64("max", 0, "Maximum allowed min value")

	checkMeanCmd.Flags().String("data", "", "Path to the data file")
	checkMeanCmd.Flags().String("column", "", "Name of the column to check")
	checkMeanCmd.Flags().Float64("min", 0, "Minimum allowed mean value")
	checkMeanCmd.Flags().Float64("max", 0, "Maximum allowed mean value")

	checkMedianCmd.Flags().String("data", "", "Path to the data file")
	checkMedianCmd.Flags().String("column", "", "Name of the column to check")
	checkMedianCmd.Flags().Float64("min", 0, "Minimum allowed median value")
	checkMedianCmd.Flags().Float64("max", 0, "Maximum allowed median value")

	checkDateFormatCmd.Flags().String("data", "", "Path to the data file")
	checkDateFormatCmd.Flags().String("column", "", "Name of the column to check")
	checkDateFormatCmd.Flags().String("format", "", "Date format (strftime)")

	checkRowCountCmd.Flags().String("data", "", "Path to the data file")
	checkRowCountCmd.Flags().Int64("min", 0, "Minimum row count")
	checkRowCountCmd.Flags().Int64("max", 0, "Maximum row count")

	checkColCountCmd.Flags().String("data", "", "Path to the data file")
	checkColCountCmd.Flags().Int("min", 0, "Minimum column count")
	checkColCountCmd.Flags().Int("max", 0, "Maximum column count")

	checkNotInSetCmd.Flags().String("data", "", "Path to the data file")
	checkNotInSetCmd.Flags().String("column", "", "Name of the column to check")
	checkNotInSetCmd.Flags().String("values", "", "Blacklisted values (comma-separated)")

	checkIncreasingCmd.Flags().String("data", "", "Path to the data file")
	checkIncreasingCmd.Flags().String("column", "", "Name of the column to check")

	checkDateParseableCmd.Flags().String("data", "", "Path to the data file")
	checkDateParseableCmd.Flags().String("column", "", "Name of the column to check")

	checkPairEqualCmd.Flags().String("data", "", "Path to the data file")
	checkPairEqualCmd.Flags().String("col1", "", "First column name")
	checkPairEqualCmd.Flags().String("col2", "", "Second column name")

	checkDistinctInSetCmd.Flags().String("data", "", "Path to the data file")
	checkDistinctInSetCmd.Flags().String("column", "", "Name of the column to check")
	checkDistinctInSetCmd.Flags().String("values", "", "Allowed values (comma-separated)")
}
