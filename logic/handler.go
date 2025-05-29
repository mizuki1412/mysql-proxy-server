package logic

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"log"
)

type MyHandler struct{}

type MyReplicationHandler struct {
	MyHandler
}

// UseDB is called for COM_INIT_DB
func (h MyHandler) UseDB(dbName string) error {
	log.Printf("Received: UseDB %s", dbName)
	return nil
}

// HandleQuery is called for COM_QUERY
func (h MyHandler) HandleQuery(query string) (*mysql.Result, error) {
	log.Printf("Received: Query: %s", query)

	if query == `select concat(@@version, ' ', @@version_comment)` {
		r, err := mysql.BuildSimpleResultset([]string{"concat(@@version, ' ', @@version_comment)"}, [][]interface{}{
			{"8.0.11"},
		}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(r), nil
	}

	return nil, nil
}

// HandleFieldList is called for COM_FIELD_LIST packets
// Note that COM_FIELD_LIST has been deprecated since MySQL 5.7.11
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html
func (h MyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	log.Printf("Received: FieldList: table=%s, fieldWildcard:%s", table, fieldWildcard)
	return nil, nil
}

// HandleStmtPrepare is called for COM_STMT_PREPARE
func (h MyHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	log.Printf("Received: StmtPrepare: %s", query)
	return 0, 0, nil, nil
}

// HandleStmtExecute is called for COM_STMT_EXECUTE
func (h MyHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	log.Printf("Received: StmtExecute: %s (args: %v)", query, args)
	return nil, nil
}

// HandleStmtClose is called for COM_STMT_CLOSE
func (h MyHandler) HandleStmtClose(context interface{}) error {
	log.Println("Received: StmtClose")
	return nil
}

// HandleRegisterSlave is called for COM_REGISTER_SLAVE
func (h MyReplicationHandler) HandleRegisterSlave(data []byte) error {
	log.Printf("Received: RegisterSlave: %x", data)
	return nil
}

// HandleBinlogDump is called for COM_BINLOG_DUMP (non-GTID)
func (h MyReplicationHandler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	log.Printf("Received: BinlogDump: pos=%s", pos.String())
	return nil, nil
}

// HandleBinlogDumpGTID is called for COM_BINLOG_DUMP_GTID
func (h MyReplicationHandler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	log.Printf("Received: BinlogDumpGTID: gtidSet=%s", gtidSet.String())
	return nil, nil
}

// HandleOtherCommand is called for commands not handled elsewhere
func (h MyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	log.Printf("Received: OtherCommand: cmd=%x, data=%x", cmd, data)
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}
