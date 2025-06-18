package logic

import (
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
	"github.com/mizuki1412/go-core-kit/v2/service/logkit"
	"github.com/spf13/cast"
	"log/slog"
	"regexp"
	"strings"
)

type MyHandler struct {
	Target *Target
	Title  string
}

type MyReplicationHandler struct {
	MyHandler
}

// UseDB is called for COM_INIT_DB
func (h MyHandler) UseDB(dbName string) error {
	logkit.Debug(h.Title+"UseDB", slog.String("name", dbName))
	return nil
}

// HandleQuery is called for COM_QUERY
func (h MyHandler) HandleQuery(query string) (*mysql.Result, error) {
	logkit.Debug(h.Title+"Query", slog.String("sql", query))
	return h.handleQuery(query, nil, false)
}

// HandleFieldList is called for COM_FIELD_LIST packets
// Note that COM_FIELD_LIST has been deprecated since MySQL 5.7.11
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html
func (h MyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	logkit.Debug(h.Title+"FieldList", slog.String("table", table), slog.String("fieldWildcard", fieldWildcard))
	return nil, nil
}

// HandleStmtPrepare is called for COM_STMT_PREPARE
func (h MyHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	logkit.Debug(h.Title+" StmtPrepare", slog.String("sql", query))
	query = strings.ToLower(query)
	params := strings.Count(query, "?")
	return params, 0, nil, nil
}

// HandleStmtExecute is called for COM_STMT_EXECUTE
func (h MyHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	logkit.Debug(h.Title+" StmtExecute", slog.String("sql", query), slog.Any("args", args))
	return h.handleQuery(query, args, true)
}

// HandleStmtClose is called for COM_STMT_CLOSE
func (h MyHandler) HandleStmtClose(context any) error {
	logkit.Debug(h.Title + " StmtClose")
	return nil
}

// HandleRegisterSlave is called for COM_REGISTER_SLAVE
func (h MyReplicationHandler) HandleRegisterSlave(data []byte) error {
	logkit.Debug(h.Title + " RegisterSlave")
	return nil
}

// HandleBinlogDump is called for COM_BINLOG_DUMP (non-GTID)
func (h MyReplicationHandler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	logkit.Debug(h.Title+" BinlogDump", slog.String("pos", pos.String()))
	return nil, nil
}

// HandleBinlogDumpGTID is called for COM_BINLOG_DUMP_GTID
func (h MyReplicationHandler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	logkit.Debug(h.Title+" BinlogDumpGTID", slog.String("gtidSet", gtidSet.String()))
	return nil, nil
}

// HandleOtherCommand is called for commands not handled elsewhere
func (h MyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	logkit.Debug(h.Title+" OtherCommand", slog.Any("cmd", cmd), slog.String("data", string(data)))
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}

func (h *MyHandler) handleQuery(query string, args []any, binary bool) (*mysql.Result, error) {
	query = strings.TrimSpace(query)
	// 去掉开头的注释
	if strings.Index(query, "/*") >= 0 {
		end := strings.Index(query, "*/")
		query = query[end+2:]
	}
	query0 := query
	query = strings.ToLower(query)
	switch {
	case strings.Index(query, "select") == 0, strings.Index(query, "show ") == 0:
		var r *mysql.Resultset
		var rows *sqlx.Rows
		var err error
		if regexp.MustCompile("select +@@").MatchString(query) {
			if query[len(query)-1] == ';' {
				query = query[:len(query)-1]
			}
			ps := strings.Split(query, ",")
			fields := make([]string, 0, 5)
			values := make([]any, 0, 5)
			for _, v := range ps {
				v = strings.TrimSpace(v)
				i := strings.Index(v, "@@")
				if i > -1 {
					v = v[i:]
					vs := strings.Split(v, " as ")
					key := strings.TrimSpace(vs[0])
					var kas string
					if len(vs) == 1 {
						kas = key
					} else {
						kas = strings.TrimSpace(vs[1])
					}
					var val any
					switch key {
					case "@@session.auto_increment_increment":
						val = 1
					case "@@character_set_client", "@@character_set_connection", "@@character_set_results", "@@character_set_server":
						val = "utf8mb4"
					case "@@collation_server", "@@collation_connection":
						val = "utf8mb4_unicode_ci"
					case "@@interactive_timeout", "@@wait_timeout":
						val = 28800
					case "@@license":
						val = "GPL"
					case "@@lower_case_table_names":
						val = 0
					case "@@max_allowed_packet":
						val = 4194304
					case "@@net_write_timeout":
						val = 60
					case "@@performance_schema":
						val = 1
					case "@@sql_mode":
						val = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
					case "@@system_time_zone":
						val = "CST"
					case "@@time_zone":
						val = "SYSTEM"
					case "@@transaction_isolation", "@@session.transaction_isolation":
						val = "REPEATABLE-READ"
					case "@@session.transaction_read_only":
						val = 0
					default:
						val = ""
					}
					fields = append(fields, kas)
					values = append(values, val)
				} else {
					logkit.Error(h.Title + "not exist @@: " + v)
				}
			}
			if len(fields) == 0 {
				err = errors.New("no fields found")
			}
			r, err = mysql.BuildSimpleResultset(fields, [][]any{
				values,
			}, binary)
		} else {
			query = h.replacePlaceholder(query0)
			rows, err = h.Target.DBPool.Queryx(query, args...)
			var ret []any
			var rets [][]any
			var columns []string
			if err == nil {
				defer rows.Close()
				columns, err = rows.Columns()
				for rows.Next() {
					ret, err = rows.SliceScan()
					if err != nil {
						break
					}
					formated := make([]any, 0, len(ret))
					for _, col := range ret {
						formated = append(formated, h.convertGoValueToMysqlValue(col))
					}
					rets = append(rets, formated)
				}
				r, err = mysql.BuildSimpleResultset(columns, rets, binary)
			}
		}
		if err != nil {
			return nil, err
		} else {
			return mysql.NewResult(r), nil
		}
	case strings.Index(query, "insert") == 0:
		res := mysql.NewResultReserveResultset(0)
		query = h.replacePlaceholder(query0)
		_, err := h.Target.DBPool.Exec(query, args...)
		if err != nil {
			return nil, err
		}
		//ri, err := r.LastInsertId()
		//res.InsertId = cast.ToUint64(ri)
		//logkit.Debug(query, slog.Any("ret-id", ri))
		return res, nil
	case strings.Index(query, "delete") == 0,
		strings.Index(query, "update") == 0,
		strings.Index(query, "replace") == 0,
		query == "rollback", query == "commit",
		query == "begin", query == "start transaction",
		strings.Index(query, "set autocommit") == 0:
		res := mysql.NewResultReserveResultset(0)
		if strings.Index(query, "set autocommit") == 0 {
			if strings.LastIndex(query, "1") >= 0 {
				return res, nil
			} else if strings.LastIndex(query, "0") >= 0 {
				// todo 暂不支持autocommit修改
				query0 = "begin"
			} else {
				logkit.Error(h.Title + "autocommit error 【" + query + "】")
				return res, nil
			}
		}
		query = h.replacePlaceholder(query0)
		r, err := h.Target.DBPool.Exec(query, args...)
		if err != nil {
			logkit.Error(h.Title + "query error 【" + query + "】" + err.Error())
			return nil, err
		}
		ri, _ := r.RowsAffected()
		res.AffectedRows = uint64(ri)
		return res, nil
	case strings.Index(query, "set ") == 0:
		res := mysql.NewResultReserveResultset(0)
		return res, nil
		// todo create
	default:
		logkit.Error(h.Title + "invalid query: " + query0)
		return nil, fmt.Errorf("invalid query %s", query0)
	}
}

func (h MyHandler) replacePlaceholder(sql string) string {
	switch h.Target.Driver {
	case DriverPostgres, DriverKingbase:
		// 替换?为$
		k := 1
		sql2 := make([]uint8, 0, len(sql))
		for i := 0; i < len(sql); i++ {
			if sql[i] == '?' {
				sql2 = append(sql2, '$', uint8(k+'0'))
				k++
			} else {
				sql2 = append(sql2, sql[i])
			}
		}
		return string(sql2)
	default:
		return sql
	}
}

func (h MyHandler) convertGoValueToMysqlValue(val any) any {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case bool:
		return cast.ToInt8(v)
	//case time.Time:
	//	return v.Format("2006-01-02 15:04:05")
	default:
		return val
	}
}
