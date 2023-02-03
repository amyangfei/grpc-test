package mysqlop

import (
	"fmt"
	"strings"
)

// EscapeName replaces all "`" in name with double "`"
func EscapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

// QuoteSchema quotes a full table name
func QuoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", EscapeName(schema), EscapeName(table))
}
