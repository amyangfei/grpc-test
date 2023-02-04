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

// QuoteName wraps a name with "`"
func QuoteName(name string) string {
	return "`" + EscapeName(name) + "`"
}

// valuesHolder gens values holder like (?,?,?).
func valuesHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 3)
	builder.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	builder.WriteByte(')')
	return builder.String()
}
