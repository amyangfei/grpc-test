package mysqlop

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	_ "github.com/pingcap/tidb/planner/core" // to setup expression.EvalAstExpr
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

type Row struct {
	values []interface{}
}

type RowChange struct {
	beforeValues []interface{}
	afterValues  []interface{}
	table        *model.TableInfo
}

func (r *RowChange) Stmt() string {
	return ""
}

func (r *RowChange) Args() []interface{} {
	return nil
}

func (r *RowChange) getWhereColumnsAndValues() ([]string, []interface{}) {
	pk := r.table.GetPrimaryKey()
	names := make([]string, 0, len(pk.Columns))
	args := make([]interface{}, 0, len(pk.Columns))
	for _, idxCol := range pk.Columns {
		names = append(names, r.table.Columns[idxCol.Offset].Name.O)
		args = append(args, r.beforeValues[idxCol.Offset])
	}
	return names, args
}

type SysbenchGen struct {
	db     *sql.DB
	schema string
	table  string
	info   *model.TableInfo
	size   int
	cache  []*Row

	next int
}

func NewSysbenchGen(
	ctx context.Context, db *sql.DB, schema, table string, size int,
) (*SysbenchGen, error) {
	gen := &SysbenchGen{
		db:     db,
		schema: schema,
		table:  table,
		size:   size,
	}
	if info, err := gen.getTableInfo(ctx, QuoteSchema(schema, table)); err != nil {
		return nil, err
	} else {
		gen.info = info
	}
	if err := gen.initCache(ctx); err != nil {
		return nil, err
	}
	return gen, nil
}

func (g *SysbenchGen) Next() *RowChange {
	row := g.cache[g.next]
	rc := &RowChange{
		beforeValues: make([]interface{}, len(row.values)),
		afterValues:  make([]interface{}, len(row.values)),
		table:        g.info,
	}
	for i, value := range row.values {
		ft := g.info.Columns[i].FieldType
		notUniqueKey := !mysql.HasPriKeyFlag(ft.GetFlag()) && !mysql.HasUniKeyFlag(ft.GetFlag())
		if notUniqueKey && ft.EvalType() == types.ETInt {
			intVal, err := strconv.Atoi(string(value.([]byte)))
			if err != nil {
				log.Panic(err)
			}
			row.values[i] = intVal + 1
			rc.beforeValues[i] = intVal
			rc.afterValues[i] = intVal + 1
		} else {
			rc.beforeValues[i] = value
			rc.afterValues[i] = value
		}
	}
	g.next = (g.next + 1) % g.size
	return rc
}

func (g *SysbenchGen) initCache(ctx context.Context) error {
	query := fmt.Sprintf("SELECT * FROM "+QuoteSchema(g.schema, g.table)+" LIMIT %d", g.size)
	rows, err := g.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	g.cache = make([]*Row, 0, g.size)
	for rows.Next() {
		row := make([]interface{}, len(g.info.Columns))
		for i := range row {
			row[i] = new(interface{})
		}
		if err := rows.Scan(row...); err != nil {
			return err
		}
		values := make([]interface{}, len(g.info.Columns))
		for i, v := range row {
			values[i] = *(v.(*interface{}))
		}
		g.cache = append(g.cache, &Row{values: values})
	}
	if len(g.cache) < g.size {
		log.Printf("table %s cached size %d is smaller than expected %d",
			QuoteSchema(g.schema, g.table), len(g.cache), g.size)
		g.size = len(g.cache)
	}
	return rows.Close()
}

func getDefaultParser(ctx context.Context, db *sql.DB, sqlMode string) (*parser.Parser, error) {
	setSQLMode := fmt.Sprintf("SET SESSION SQL_MODE = '%s'", mysql.DefaultSQLMode)
	if _, err := db.ExecContext(ctx, setSQLMode); err != nil {
		return nil, err
	}
	mode, err := mysql.GetSQLMode(sqlMode)
	if err != nil {
		return nil, err
	}
	parser2 := parser.New()
	parser2.SetSQLMode(mode)
	return parser2, nil
}

func (gen *SysbenchGen) getTableInfo(ctx context.Context, schemaTable string) (*model.TableInfo, error) {
	createTableSQL, err := gen.getCreateTableSQL(ctx, schemaTable)
	if err != nil {
		return nil, err
	}

	stmtParser, err := getDefaultParser(ctx, gen.db, mysql.DefaultSQLMode)
	if err != nil {
		return nil, err
	}
	stmtNode, err := stmtParser.ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, err
	}

	dbSession := mock.NewContext()
	dbSession.GetSessionVars().StrictSQLMode = false
	ti, err := ddl.BuildTableInfoWithStmt(dbSession, stmtNode.(*ast.CreateTableStmt), mysql.DefaultCharset, "", nil)
	if err != nil {
		return nil, err
	}
	ti.State = model.StatePublic
	return ti, nil
}

func (gen *SysbenchGen) getCreateTableSQL(ctx context.Context, schemaTable string) (string, error) {
	querySQL := fmt.Sprintf("SHOW CREATE TABLE %s", schemaTable)
	var table, createStr string

	rows, err := gen.db.QueryContext(ctx, querySQL)
	if err != nil {
		return "", err
	}

	defer rows.Close()
	if rows.Next() {
		if scanErr := rows.Scan(&table, &createStr); scanErr != nil {
			return "", err
		}
	} else {
		return "", err
	}

	if err = rows.Close(); err != nil {
		return "", err
	}
	return createStr, nil
}
