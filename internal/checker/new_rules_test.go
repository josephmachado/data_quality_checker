package checker

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempCSV(t *testing.T, content string) string {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "test.csv")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNewRules(t *testing.T) {
	checker, _ := setup(t)

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
