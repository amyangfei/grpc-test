package mysqlop

import "fmt"

const (
	TableSchema = "create table `%s`.`%s` " +
		"(id bigint primary key, a1 varchar(50), a2 varchar(50), a3 varchar(50));"
	varcharSize = 50
)

type TableGen struct {
	schema    string
	table     string
	startID   int
	insertSQL string
}

func NewTableGen(schema string, table string, id int) *TableGen {
	gen := &TableGen{
		schema:  schema,
		table:   table,
		startID: id,
	}
	gen.insertSQL = fmt.Sprintf(
		"insert into `%s`.`%s` (id,a1,a2,a3) values (?,?,?,?)",
		schema, table,
	)
	return gen
}

func (g *TableGen) Next() *DML {
	dml := &DML{
		sql: g.insertSQL,
		args: []interface{}{
			g.startID,
			RandStringBytesMaskImprSrc(varcharSize),
			RandStringBytesMaskImprSrc(varcharSize),
			RandStringBytesMaskImprSrc(varcharSize),
		},
	}
	g.startID++
	return dml
}
