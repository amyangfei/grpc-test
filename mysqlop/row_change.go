package mysqlop

import (
	"fmt"
	"log"
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

func GenBatchUpdateSQL(changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		log.Panic("row changes is empty")
		return "", nil
	}
	var buf strings.Builder
	buf.Grow(1024)

	// Generate UPDATE `db`.`table` SET
	first := changes[0]
	buf.WriteString("UPDATE ")
	buf.WriteString(QuoteSchema(first.schema, first.table))
	buf.WriteString(" SET ")

	// Pre-generate essential sub statements used after WHEN, WHERE and IN.
	var (
		whereCaseStmt string
		whenCaseStmt  string
		inCaseStmt    string
	)
	whereColumns, _ := first.getWhereColumnsAndValues()
	if len(whereColumns) == 1 {
		// one field PK or UK, use `field`=? directly.
		whereCaseStmt = QuoteName(whereColumns[0])
		whenCaseStmt = whereCaseStmt + "=?"
		inCaseStmt = valuesHolder(len(changes))
	} else {
		// multiple fields PK or UK, use ROW(...fields) expression.
		whereValuesHolder := valuesHolder(len(whereColumns))
		whereCaseStmt = "ROW("
		for i, column := range whereColumns {
			whereCaseStmt += QuoteName(column)
			if i != len(whereColumns)-1 {
				whereCaseStmt += ","
			} else {
				whereCaseStmt += ")"
				whenCaseStmt = whereCaseStmt + "=ROW" + whereValuesHolder
			}
		}
		var inCaseStmtBuf strings.Builder
		// inCaseStmt sample:     IN (ROW(?,?,?),ROW(?,?,?))
		//                           ^                     ^
		// Buffer size count between |---------------------|
		// equals to 3 * len(changes) for each `ROW`
		// plus 1 * len(changes) - 1  for each `,` between every two ROW(?,?,?)
		// plus len(whereValuesHolder) * len(changes)
		// plus 2 for `(` and `)`
		inCaseStmtBuf.Grow((4+len(whereValuesHolder))*len(changes) + 1)
		inCaseStmtBuf.WriteString("(")
		for i := range changes {
			inCaseStmtBuf.WriteString("ROW")
			inCaseStmtBuf.WriteString(whereValuesHolder)
			if i != len(changes)-1 {
				inCaseStmtBuf.WriteString(",")
			} else {
				inCaseStmtBuf.WriteString(")")
			}
		}
		inCaseStmt = inCaseStmtBuf.String()
	}

	// Generate `ColumnName`=CASE WHEN .. THEN .. END
	// Use this value in order to identify which is the first CaseWhenThen line,
	// because generated column can happen any where and it will be skipped.
	isFirstCaseWhenThenLine := true
	for _, column := range first.info.Columns {
		if isGenerated(first.info.Columns, column.Name) {
			continue
		}
		if !isFirstCaseWhenThenLine {
			// insert ", " after END of each lines except for the first line.
			buf.WriteString(", ")
		}

		buf.WriteString(QuoteName(column.Name.String()) + "=CASE")
		for range changes {
			buf.WriteString(" WHEN ")
			buf.WriteString(whenCaseStmt)
			buf.WriteString(" THEN ?")
		}
		buf.WriteString(" END")
		isFirstCaseWhenThenLine = false
	}

	// Generate WHERE .. IN ..
	buf.WriteString(" WHERE ")
	buf.WriteString(whereCaseStmt)
	buf.WriteString(" IN ")
	buf.WriteString(inCaseStmt)

	// Build args of the UPDATE SQL
	var assignValueColumnCount int
	var skipColIdx []int
	for i, col := range first.info.Columns {
		if isGenerated(first.info.Columns, col.Name) {
			skipColIdx = append(skipColIdx, i)
			continue
		}
		assignValueColumnCount++
	}
	args := make([]any, 0,
		assignValueColumnCount*len(changes)*(len(whereColumns)+1)+len(changes)*len(whereColumns))
	argsPerCol := make([][]any, assignValueColumnCount)
	for i := 0; i < assignValueColumnCount; i++ {
		argsPerCol[i] = make([]any, 0, len(changes)*(len(whereColumns)+1))
	}
	whereValuesAtTheEnd := make([]any, 0, len(changes)*len(whereColumns))
	for _, change := range changes {
		_, whereValues := change.getWhereColumnsAndValues()
		// a simple check about different number of WHERE values, not trying to
		// cover all cases
		if len(whereValues) != len(whereColumns) {
			log.Panicf("len(whereValues) %d != len(whereColumns) %d", len(whereValues), len(whereColumns))
			return "", nil
		}

		whereValuesAtTheEnd = append(whereValuesAtTheEnd, whereValues...)

		i := 0 // used as index of skipColIdx
		writeableCol := 0
		for j, val := range change.afterValues {
			if i < len(skipColIdx) && skipColIdx[i] == j {
				i++
				continue
			}
			argsPerCol[writeableCol] = append(argsPerCol[writeableCol], whereValues...)
			argsPerCol[writeableCol] = append(argsPerCol[writeableCol], val)
			writeableCol++
		}
	}
	for _, a := range argsPerCol {
		args = append(args, a...)
	}
	args = append(args, whereValuesAtTheEnd...)

	return buf.String(), args
}
