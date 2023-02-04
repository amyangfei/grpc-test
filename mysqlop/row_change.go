package mysqlop

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser/model"
)

type RowChange struct {
	beforeValues []interface{}
	afterValues  []interface{}
	schema       string
	table        string
	info         *model.TableInfo
}

func (r *RowChange) StmtAndArgs() (string, []interface{}) {
	return r.genUpdateSQL()
}

func (r *RowChange) getWhereColumnsAndValues() ([]string, []interface{}) {
	/* FIXME: indices is missing when PKIsHandle in TiDB
	pk := r.info.GetPrimaryKey()
	names := make([]string, 0, len(pk.Columns))
	args := make([]interface{}, 0, len(pk.Columns))
	for _, idxCol := range pk.Columns {
		names = append(names, r.info.Columns[idxCol.Offset].Name.O)
		args = append(args, r.beforeValues[idxCol.Offset])
	}
	return names, args
	*/
	pkIndex := 0
	names := []string{r.info.Columns[pkIndex].Name.O}
	args := []interface{}{r.beforeValues[pkIndex]}
	return names, args
}

func (r *RowChange) genUpdateSQL() (string, []interface{}) {
	var buf strings.Builder
	buf.Grow(2048)
	buf.WriteString("UPDATE ")
	buf.WriteString(QuoteSchema(r.schema, r.table))
	buf.WriteString(" SET ")

	args := make([]interface{}, 0, len(r.beforeValues)+len(r.afterValues))
	writtenFirstCol := false
	for i, col := range r.info.Columns {
		if isGenerated(r.info.Columns, col.Name) {
			continue
		}

		if writtenFirstCol {
			buf.WriteString(", ")
		}
		writtenFirstCol = true
		fmt.Fprintf(&buf, "%s = ?", QuoteName(col.Name.O))
		args = append(args, r.afterValues[i])
	}

	buf.WriteString(" WHERE ")
	whereArgs := r.genWhere(&buf)
	buf.WriteString(" LIMIT 1")

	args = append(args, whereArgs...)
	return buf.String(), args
}

func (r *RowChange) genWhere(buf *strings.Builder) []interface{} {
	whereColumns, whereValues := r.getWhereColumnsAndValues()

	for i, col := range whereColumns {
		if i != 0 {
			buf.WriteString(" AND ")
		}
		buf.WriteString(QuoteName(col))
		if whereValues[i] == nil {
			buf.WriteString(" IS ?")
		} else {
			buf.WriteString(" = ?")
		}
	}
	return whereValues
}

func isGenerated(columns []*model.ColumnInfo, name model.CIStr) bool {
	for _, col := range columns {
		if col.Name.L == name.L {
			return col.IsGenerated()
		}
	}
	return false
}
