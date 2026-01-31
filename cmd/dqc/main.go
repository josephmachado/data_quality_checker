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
	rootCmd.AddCommand(showLogsCmd)
	rootCmd.AddCommand(cleanLogsCmd)
}

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
}
