package logic

import (
	"database/sql"
	"fmt"
	sqle "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/mizuki1412/go-core-kit/v2/class/exception"
	"github.com/mizuki1412/go-core-kit/v2/service/logkit"
	"io"
	"time"
)

type DBProvider struct {
	Driver string
	Db     string
	Schema string
	db     *sql.DB
}

func (b DBProvider) Database(ctx *sqle.Context, name string) (sqle.Database, error) {
	switch b.Driver {
	case DriverKingbase:
		fallthrough
	case DriverPostgres:
		return &PGDatabase{
			name:     name,
			provider: &b,
		}, nil
	case DriverMysql:
		// todo
		fallthrough
	default:
		panic(exception.New("driver error"))
	}
}

func (b DBProvider) HasDatabase(ctx *sqle.Context, name string) bool {
	return true
}

func (b DBProvider) AllDatabases(ctx *sqle.Context) []sqle.Database {
	//TODO
	//db, _ := b.PGDatabase(ctx, "myspace")
	return []sqle.Database{}
}

type PGDatabase struct {
	name     string
	provider *DBProvider
}

func (d *PGDatabase) Name() string { return d.name }
func (d *PGDatabase) Tables() map[string]sqle.Table {
	logkit.Debug("pgdb: Tables()")
	rows, err := d.provider.db.Query(fmt.Sprintf(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '%s'
    `, d.provider.Schema))
	if err != nil {
		return nil
	}
	defer rows.Close()

	tables := make(map[string]sqle.Table)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		tables[name] = &PostgresTable{
			db:   d,
			name: name,
		}
	}

	return tables
}
func (d *PGDatabase) GetTableInsensitive(ctx *sqle.Context, tblName string) (sqle.Table, bool, error) {
	// 检查表是否存在
	exists, err := d.tableExists(tblName)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}

	return &PostgresTable{
		db:   d,
		name: tblName,
	}, true, nil
}

func (d *PGDatabase) GetTableNames(ctx *sqle.Context) ([]string, error) {
	logkit.Debug("pgdb: GetTableNames()")
	// 查询PostgreSQL获取所有表名
	rows, err := d.provider.db.Query(fmt.Sprintf(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '%s'
    `, d.provider.Schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		tables = append(tables, name)
	}
	return tables, nil
}

func (d *PGDatabase) tableExists(name string) (bool, error) {
	// kingbase中一般用户可能没权限查information_schema.tables
	//var exists bool
	//err := d.provider.db.QueryRow(fmt.Sprintf(`
	//    SELECT EXISTS (
	//        SELECT 1
	//        FROM information_schema.tables
	//        WHERE table_schema = '%s'
	//        AND table_name = $1
	//    )`, d.provider.Schema), name).Scan(&exists)
	//return exists, err
	return true, nil
}

type PostgresTable struct {
	db     *PGDatabase
	name   string
	schema sqle.Schema
}

func (t *PostgresTable) String() string {
	return t.name
}

func (t *PostgresTable) Collation() sqle.CollationID {
	//TODO implement me
	panic("implement me")
}

func (t *PostgresTable) Name() string { return t.name }

func (t *PostgresTable) Schema() sqle.Schema {
	logkit.Debug("pgtable: Schema()")
	if t.schema != nil {
		return t.schema
	}

	// 查询PostgreSQL获取表结构
	rows, err := t.db.provider.db.Query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = $1
    `, t.name)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var schema sqle.Schema
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			continue
		}

		// 将PostgreSQL类型转换为MySQL类型
		mysqlType := pgTypeToMySQLType(typ)
		schema = append(schema, &sqle.Column{
			Name: name,
			Type: mysqlType,
		})
	}

	t.schema = schema
	return schema
}

func pgTypeToMySQLType(pgType string) sqle.Type {
	switch pgType {
	case "integer", "int4":
		return types.Int32
	case "bigint", "int8":
		return types.Int64
	case "boolean":
		return types.Boolean
	case "timestamp", "timestamptz":
		return types.Timestamp
	case "float8":
		return types.Float64
	default:
		return types.Text
	}
}

func (t *PostgresTable) Partitions(ctx *sqle.Context) (sqle.PartitionIter, error) {
	return &singlePartitionIter{}, nil
}

func (t *PostgresTable) PartitionRows(ctx *sqle.Context, p sqle.Partition) (sqle.RowIter, error) {
	logkit.Debug("pgtable: PartitionRows()")
	// 构建SELECT查询
	query := fmt.Sprintf("SELECT * FROM %s", t.name)

	// 执行PostgreSQL查询
	rows, err := t.db.provider.db.Query(query)
	if err != nil {
		return nil, err
	}

	return &postgresRowIter{
		rows:   rows,
		schema: t.Schema(),
	}, nil
}

// singlePartition 表示一个单一分区
type singlePartition struct{}

// Key 返回分区键
func (p *singlePartition) Key() []byte { return []byte("") }

// singlePartitionIter 是单一分区的迭代器
type singlePartitionIter struct {
	done bool
}

// Next 实现 PartitionIter 接口
func (i *singlePartitionIter) Next(ctx *sqle.Context) (sqle.Partition, error) {
	if i.done {
		return nil, io.EOF
	}
	i.done = true
	return &singlePartition{}, nil
}

// Close 实现 PartitionIter 接口
func (i *singlePartitionIter) Close(ctx *sqle.Context) error { return nil }

type postgresRowIter struct {
	rows   *sql.Rows
	schema sqle.Schema
}

func (i *postgresRowIter) Next(ctx *sqle.Context) (sqle.Row, error) {
	logkit.Debug("pgtable: Next()")
	if !i.rows.Next() {
		return nil, io.EOF
	}

	// 创建值的切片
	values := make([]interface{}, len(i.schema))
	for j := range values {
		values[j] = new(interface{})
	}

	// 扫描行数据
	if err := i.rows.Scan(values...); err != nil {
		return nil, err
	}

	// 转换为MySQL兼容格式
	row := make([]interface{}, len(values))
	for j, val := range values {
		v := *(val.(*interface{}))
		row[j] = convertPgValueToMySQL(v, i.schema[j].Type)
	}

	return sqle.NewRow(row...), nil
}

func convertPgValueToMySQL(val interface{}, typ sqle.Type) interface{} {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case time.Time:
		if _, ok := typ.(sqle.DatetimeType); ok {
			return v.Format("2006-01-02 15:04:05")
		}
		return v
	default:
		return val
	}
}

func (i *postgresRowIter) Close(ctx *sqle.Context) error {
	return i.rows.Close()
}
