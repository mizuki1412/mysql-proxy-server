package logic

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
	"github.com/mizuki1412/go-core-kit/v2/service/logkit"
	"github.com/spf13/cast"
	"log"
	"log/slog"
	"strings"
)

type MyHandler struct {
	Target *Target
}

type MyReplicationHandler struct {
	MyHandler
}

// UseDB is called for COM_INIT_DB
func (h MyHandler) UseDB(dbName string) error {
	logkit.Debug("Received: UseDB", slog.String("name", dbName))
	return nil
}

// HandleQuery is called for COM_QUERY
func (h MyHandler) HandleQuery(query string) (*mysql.Result, error) {
	logkit.Debug("Received: Query", slog.String("sql", query))
	return h.handleQuery(query, nil, false)
}

// HandleFieldList is called for COM_FIELD_LIST packets
// Note that COM_FIELD_LIST has been deprecated since MySQL 5.7.11
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html
func (h MyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	logkit.Debug("Received: FieldList", slog.String("table", table), slog.String("fieldWildcard", fieldWildcard))
	return nil, nil
}

// HandleStmtPrepare is called for COM_STMT_PREPARE
func (h MyHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	logkit.Debug("Received: StmtPrepare", slog.String("sql", query))
	query = strings.ToLower(query)
	params := strings.Count(query, "?")
	return params, 0, nil, nil
}

// HandleStmtExecute is called for COM_STMT_EXECUTE
func (h MyHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	logkit.Debug("Received: StmtExecute", slog.String("sql", query), slog.Any("args", args))
	return h.handleQuery(query, args, true)
}

// HandleStmtClose is called for COM_STMT_CLOSE
func (h MyHandler) HandleStmtClose(context any) error {
	logkit.Debug("Received: StmtClose")
	return nil
}

// HandleRegisterSlave is called for COM_REGISTER_SLAVE
func (h MyReplicationHandler) HandleRegisterSlave(data []byte) error {
	logkit.Debug("Received: RegisterSlave")
	return nil
}

// HandleBinlogDump is called for COM_BINLOG_DUMP (non-GTID)
func (h MyReplicationHandler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	logkit.Debug("Received: BinlogDump", slog.String("pos", pos.String()))
	return nil, nil
}

// HandleBinlogDumpGTID is called for COM_BINLOG_DUMP_GTID
func (h MyReplicationHandler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	logkit.Debug("Received: BinlogDumpGTID", slog.String("gtidSet", gtidSet.String()))
	return nil, nil
}

// HandleOtherCommand is called for commands not handled elsewhere
func (h MyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	logkit.Debug("Received: OtherCommand", slog.Any("cmd", cmd), slog.String("data", string(data)))
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}

func (h *MyHandler) handleQuery(query string, args []any, binary bool) (*mysql.Result, error) {
	query = strings.TrimSpace(query)
	query = strings.ToLower(query)
	ss := strings.Split(query, " ")
	switch ss[0] {
	case "select":
		var r *mysql.Resultset
		var rows *sqlx.Rows
		var err error
		if strings.Contains(query, "max_allowed_packet") {
			r, err = mysql.BuildSimpleResultset([]string{"@@max_allowed_packet"}, [][]any{
				{mysql.MaxPayloadLen},
			}, binary)
		} else if strings.Contains(query, "concat(@@version, ' ', @@version_comment)") {
			r, err = mysql.BuildSimpleResultset([]string{"concat(@@version, ' ', @@version_comment)"}, [][]any{
				{"8.0.11"},
			}, binary)
		} else {
			// todo
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
					rets = append(rets, ret)
				}
				r, err = mysql.BuildSimpleResultset(columns, rets, binary)
			}
		}
		if err != nil {
			return nil, err
		} else {
			return mysql.NewResult(r), nil
		}
	case "insert":
		res := mysql.NewResultReserveResultset(0)
		// todo
		query = strings.ReplaceAll(query, "?", "$1")
		log.Println(query)
		r, err := h.Target.DBPool.Exec(query, args...)
		if err != nil {
			return nil, err
		}
		ri, err := r.LastInsertId()
		res.InsertId = cast.ToUint64(ri)
		return res, nil
	case "delete", "update", "replace":
		res := mysql.NewResultReserveResultset(0)
		// todo
		res.AffectedRows = 1
		return res, nil
	default:
		return nil, fmt.Errorf("invalid query %s", query)
	}
}

func (h MyHandler) ReplacePlaceholder(sql string) string {
	switch h.Target.Driver {
	case DriverPostgres, DriverKingbase:
		// todo
		return sql
	default:
		return sql
	}
}
