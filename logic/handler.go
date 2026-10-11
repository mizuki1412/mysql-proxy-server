package logic

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/example/go-frame/pkg/service/logkit"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cast"
)

// 对外模拟的 MySQL 版本号，握手应答与 version()/@@"version" 保持一致
const mysqlServerVersion = "8.0.11"

// 金仓没有 MySQL 系统变量，这些值由代理直接应答（覆盖 Connector/J 等
// 客户端连接建立时的 bootstrap 查询）
var sessionVars = map[string]any{
	"auto_increment_increment": 1,
	"character_set_client":     "utf8",
	"character_set_connection": "utf8",
	"character_set_results":    "utf8",
	"character_set_server":     "utf8",
	"collation_server":         "utf8_general_ci",
	"collation_connection":     "utf8_general_ci",
	"interactive_timeout":      28800,
	"wait_timeout":             28800,
	"license":                  "GPL",
	"lower_case_table_names":   0,
	"max_allowed_packet":       4194304,
	"net_write_timeout":        60,
	"performance_schema":       1,
	"sql_mode":                 "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION",
	"system_time_zone":         "CST",
	"time_zone":                "SYSTEM",
	"transaction_isolation":    "REPEATABLE-READ",
	"transaction_read_only":    0,
	"version":                  mysqlServerVersion,
	"version_comment":          "mysql-proxy-server (kingbase)",
}

// 会话级状态，一个客户端连接一份
type sessionState struct {
	lastInsertID uint64            // 最近一次 insert 的自增 id，供 last_insert_id() 应答
	inTx         bool              // begin/autocommit=0 之后处于事务中
	pkCache      map[string]string // 表名 -> 自增主键列
	pkDisabled   bool              // information_schema 查询失败后整体停用，避免反复报错
}

type MyHandler struct {
	Target Target
	Title  string
	state  *sessionState
}

type MyReplicationHandler struct {
	MyHandler
}

// UseDB is called for COM_INIT_DB
func (h *MyHandler) UseDB(dbName string) error {
	logkit.Debug(h.Title+"UseDB", slog.String("name", dbName))
	return nil
}

// HandleQuery is called for COM_QUERY
func (h *MyHandler) HandleQuery(query string) (*mysql.Result, error) {
	querySql, lower := normalizeSQL(query)
	logkit.Debug(h.Title+"Query", slog.String("sql", querySql))
	if querySql == "" {
		return mysql.NewResultReserveResultset(0), nil
	}
	return h.execute(querySql, lower, nil, false)
}

// HandleFieldList is called for COM_FIELD_LIST packets
// Note that COM_FIELD_LIST has been deprecated since MySQL 5.7.11
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html
func (h *MyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	logkit.Debug(h.Title+"FieldList", slog.String("table", table), slog.String("fieldWildcard", fieldWildcard))
	return nil, nil
}

// HandleStmtPrepare is called for COM_STMT_PREPARE
// 通过后端预编译拿到真实的参数个数与列数；金仓不支持的语句回退为文本协议
func (h *MyHandler) HandleStmtPrepare(query string) (int, int, any, error) {
	querySql, lower := normalizeSQL(query)
	logkit.Debug(h.Title+" StmtPrepare", slog.String("sql", querySql))
	params := countPlaceholders(querySql)
	columns := 0
	if hasPrefix(lower, backendPreparablePrefixes) {
		rewritten := h.replacePlaceholders(querySql)
		st, err := h.Target.DBPool.Preparex(rewritten)
		if err == nil {
			p, c := h.probePrepareMetadata(rewritten)
			if p >= 0 {
				if p != params {
					logkit.Error(h.Title + " placeholder count mismatch: client-side=" +
						strconv.Itoa(params) + " backend=" + strconv.Itoa(p))
				}
				params = p
			}
			columns = c
			return params, columns, &preparedStmt{st: st, sql: rewritten}, nil
		}
		logkit.Error(h.Title + " backend prepare failed, fallback to text mode: " + err.Error())
	}
	return params, columns, nil, nil
}

// preparedStmt 后端预编译语句，作为 COM_STMT_PREPARE/EXECUTE/CLOSE 之间传递的 context。
// database/sql 不暴露预编译元数据，也拿不到原始 SQL，这里自己包装。
type preparedStmt struct {
	st  *sqlx.Stmt
	sql string // 占位符改写后的 SQL
}

// probePrepareMetadata 通过底层驱动连接预编译同一语句，读取参数个数与结果列数。
// 独立于 Preparex 多一次 describe 往返（驱动接口不暴露这些元数据）
func (h *MyHandler) probePrepareMetadata(rewritten string) (params, columns int) {
	params, columns = -1, 0
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dbConn, err := h.Target.DBPool.Conn(ctx)
	if err != nil {
		return
	}
	defer dbConn.Close()
	_ = dbConn.Raw(func(driverConn any) error {
		var dst driver.Stmt
		if pcc, ok := driverConn.(driver.ConnPrepareContext); ok {
			dst, err = pcc.PrepareContext(ctx, rewritten)
		} else if pc, ok := driverConn.(driver.Conn); ok {
			dst, err = pc.Prepare(rewritten)
		}
		if err != nil || dst == nil {
			return nil
		}
		defer dst.Close()
		params = dst.NumInput()
		if c, ok := dst.(interface{ Columns() []string }); ok {
			columns = len(c.Columns())
		}
		return nil
	})
	return
}

// HandleStmtExecute is called for COM_STMT_EXECUTE
func (h *MyHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	args = convertArgs(args)
	if p, ok := context.(*preparedStmt); ok {
		logkit.Debug(h.Title+" StmtExecute", slog.String("sql", p.sql), slog.Any("args", args))
		return h.execPreparedStmt(p, args)
	}
	// 预编译兜底路径：与文本协议同一条执行链
	querySql, lower := normalizeSQL(query)
	logkit.Debug(h.Title+" StmtExecute", slog.String("sql", querySql), slog.Any("args", args))
	if querySql == "" {
		return mysql.NewResultReserveResultset(0), nil
	}
	return h.execute(querySql, lower, args, true)
}

// HandleStmtClose is called for COM_STMT_CLOSE
func (h *MyHandler) HandleStmtClose(context any) error {
	logkit.Debug(h.Title + " StmtClose")
	if p, ok := context.(*preparedStmt); ok {
		return p.st.Close()
	}
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
	return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "replication is not supported")
}

// HandleBinlogDumpGTID is called for COM_BINLOG_DUMP_GTID
func (h MyReplicationHandler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	logkit.Debug(h.Title+" BinlogDumpGTID", slog.String("gtidSet", gtidSet.String()))
	return nil, mysql.NewError(mysql.ER_UNKNOWN_ERROR, "replication is not supported")
}

// HandleOtherCommand is called for commands not handled elsewhere
func (h *MyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	switch cmd {
	case mysql.COM_CHANGE_USER:
		// PDO 持久连接复用时会发送 COM_CHANGE_USER 重新认证。
		// 代理为单账号模式且客户端已在本连接通过认证，直接应答 OK。
		logkit.Debug(h.Title + " ChangeUser(ignored)")
		return nil
	case mysql.COM_RESET_CONNECTION:
		// .NET MySqlConnector 连接池复用时依赖此命令重置会话
		logkit.Debug(h.Title + " ResetConnection")
		h.resetSession()
		return nil
	}
	logkit.Debug(h.Title+" OtherCommand", slog.Any("cmd", cmd), slog.String("data", string(data)))
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}

func (h *MyHandler) resetSession() {
	h.state.lastInsertID = 0
	if h.state.inTx {
		if _, err := h.Target.DBPool.Exec("rollback"); err != nil {
			logkit.Error(h.Title + " rollback on reset: " + err.Error())
		}
		h.state.inTx = false
	}
}

// ---------- SQL 路由 ----------

// 返回结果集的语句前缀
var queryPrefixes = []string{"select", "show", "desc", "describe", "explain", "with", "table", "values", "call"}

// 允许在后端预编译的语句前缀（insert 由代理改写 RETURNING，走文本协议）
var backendPreparablePrefixes = []string{"select", "with", "update", "delete"}

func hasPrefix(lower string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
}

func (h *MyHandler) execute(querySql, lower string, args []any, binary bool) (*mysql.Result, error) {
	switch {
	case hasPrefix(lower, queryPrefixes):
		if res, handled, err := h.fakeSelectResult(lower, binary); handled {
			return res, err
		}
		rows, err := h.Target.DBPool.Queryx(h.replacePlaceholders(querySql), args...)
		if err != nil {
			logkit.Error(h.Title + " query error 【" + querySql + "】" + err.Error())
			return nil, err
		}
		return h.buildResultsetFromRows(rows, binary)
	case strings.HasPrefix(lower, "use "):
		return h.handleUse(querySql)
	case strings.HasPrefix(lower, "insert"):
		return h.handleInsert(querySql, lower, args)
	case strings.HasPrefix(lower, "begin"), strings.HasPrefix(lower, "start transaction"),
		strings.HasPrefix(lower, "commit"), strings.HasPrefix(lower, "rollback"),
		strings.HasPrefix(lower, "savepoint "), strings.HasPrefix(lower, "release savepoint"):
		return h.handleTx(querySql, lower)
	case strings.HasPrefix(lower, "set"):
		return h.handleSet(lower)
	default:
		// update/delete/replace/DDL(analyze/optimize/flush/grant...) 等一律透传后端执行
		r, err := h.Target.DBPool.Exec(h.replacePlaceholders(querySql), args...)
		if err != nil {
			logkit.Error(h.Title + " exec error 【" + querySql + "】" + err.Error())
			return nil, err
		}
		return h.fillExecResult(r)
	}
}

// execPreparedStmt 执行后端预编译语句（HandleStmtPrepare 返回的 context）
func (h *MyHandler) execPreparedStmt(p *preparedStmt, args []any) (*mysql.Result, error) {
	lower := strings.ToLower(p.sql)
	if hasPrefix(lower, queryPrefixes) {
		rows, err := p.st.Queryx(args...)
		if err != nil {
			logkit.Error(h.Title + " stmt query error 【" + p.sql + "】" + err.Error())
			return nil, err
		}
		return h.buildResultsetFromRows(rows, true)
	}
	r, err := p.st.Exec(args...)
	if err != nil {
		logkit.Error(h.Title + " stmt exec error 【" + p.sql + "】" + err.Error())
		return nil, err
	}
	return h.fillExecResult(r)
}

// buildResultsetFromRows 把后端查询结果转成 MySQL 协议结果集
func (h *MyHandler) buildResultsetFromRows(rows *sqlx.Rows, binary bool) (*mysql.Result, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	rets := make([][]any, 0, 16)
	for rows.Next() {
		ret, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}
		formated := make([]any, 0, len(ret))
		for _, col := range ret {
			formated = append(formated, h.convertGoValueToMysqlValue(col))
		}
		rets = append(rets, formated)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r, err := mysql.BuildSimpleResultset(columns, rets, binary)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(r), nil
}

// fillExecResult 把 Exec 结果转成 MySQL OK 包，并缓存 insert 自增 id
func (h *MyHandler) fillExecResult(r sql.Result) (*mysql.Result, error) {
	res := mysql.NewResultReserveResultset(0)
	if ri, err := r.RowsAffected(); err == nil {
		res.AffectedRows = uint64(ri)
	}
	if id, err := r.LastInsertId(); err == nil && id > 0 {
		res.InsertId = uint64(id)
		h.state.lastInsertID = uint64(id)
	}
	return res, nil
}

// ---------- 事务 / autocommit ----------

func (h *MyHandler) handleTx(querySql, lower string) (*mysql.Result, error) {
	// 客户端连接与后端连接是一对一绑定（见 OpenDBConnection），
	// 文本 begin/commit/rollback 因此落在同一条物理连接上
	if _, err := h.Target.DBPool.Exec(querySql); err != nil {
		logkit.Error(h.Title + " tx error 【" + querySql + "】" + err.Error())
		return nil, err
	}
	switch {
	case strings.HasPrefix(lower, "begin"), strings.HasPrefix(lower, "start transaction"):
		h.state.inTx = true
	case strings.HasPrefix(lower, "commit"):
		h.state.inTx = false
	case strings.HasPrefix(lower, "rollback") && !strings.HasPrefix(lower, "rollback to"):
		// rollback to savepoint 不结束事务
		h.state.inTx = false
	}
	return mysql.NewResultReserveResultset(0), nil
}

func (h *MyHandler) handleSet(lower string) (*mysql.Result, error) {
	// autocommit 需要真实映射到后端事务，其余 SET（names/sql_mode/...）
	// 金仓侧可能无法解析，统一吞掉并应答 OK，保证客户端连接流程不被打断
	if on, matched := parseAutocommitValue(lower); matched {
		if on {
			if h.state.inTx {
				if _, err := h.Target.DBPool.Exec("commit"); err != nil {
					return nil, err
				}
				h.state.inTx = false
			}
		} else if !h.state.inTx {
			if _, err := h.Target.DBPool.Exec("begin"); err != nil {
				return nil, err
			}
			h.state.inTx = true
		}
	} else {
		logkit.Debug(h.Title + " set(ignored): " + lower)
	}
	return mysql.NewResultReserveResultset(0), nil
}

// parseAutocommitValue 解析 set [@@session.|session|global] autocommit = x
func parseAutocommitValue(lower string) (on bool, matched bool) {
	s := strings.TrimSpace(strings.TrimPrefix(lower, "set"))
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "@@session.")
	s = strings.TrimPrefix(s, "@@global.")
	s = strings.TrimPrefix(s, "@@")
	s = strings.TrimPrefix(s, "session ")
	s = strings.TrimPrefix(s, "global ")
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "autocommit") {
		return false, false
	}
	s = strings.TrimSpace(strings.TrimPrefix(s, "autocommit"))
	if !strings.HasPrefix(s, "=") {
		return false, false
	}
	s = strings.TrimSpace(s[1:])
	switch s {
	case "1", "on", "true", "default":
		return true, true
	case "0", "off", "false":
		return false, true
	}
	return false, false
}

func (h *MyHandler) handleUse(querySql string) (*mysql.Result, error) {
	// 会话内切换 schema，映射为后端 search_path；单库代理无需真正切库
	schema := strings.TrimSpace(querySql[len("use "):])
	if schema == "" {
		return mysql.NewResultReserveResultset(0), nil
	}
	for i := 0; i < len(schema); i++ {
		c := schema[i]
		if !(c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return nil, fmt.Errorf("unsupported schema name in USE: %s", schema)
		}
	}
	switch h.Target.Driver {
	case DriverKingbase, DriverPostgres:
		if _, err := h.Target.DBPool.Exec("set search_path to " + schema); err != nil {
			return nil, err
		}
		// schema 切换后自增主键缓存作废
		h.state.pkCache = map[string]string{}
	}
	return mysql.NewResultReserveResultset(0), nil
}

// ---------- 客户端可见的模拟查询 ----------

var reSelectVars = regexp.MustCompile(`(?is)^select\s+@@`)
var reSelectVersion = regexp.MustCompile(`(?is)^select\s+version\s*\(\s*\)`)
var reSelectLastInsertID = regexp.MustCompile(`(?is)^select\s+last_insert_id\s*\(\s*\)`)

// fakeSelectResult 拦截金仓无法应答、必须由代理模拟的查询
func (h *MyHandler) fakeSelectResult(lower string, binary bool) (*mysql.Result, bool, error) {
	switch {
	case reSelectVars.MatchString(lower):
		r, err := h.buildVariablesResultset(lower, binary)
		return r, true, err
	case reSelectVersion.MatchString(lower):
		r, err := buildSingleValueResultset("VERSION()", mysqlServerVersion, binary)
		return r, true, err
	case reSelectLastInsertID.MatchString(lower):
		r, err := buildSingleValueResultset("LAST_INSERT_ID()", int64(h.state.lastInsertID), binary)
		return r, true, err
	}
	return nil, false, nil
}

func buildSingleValueResultset(name string, val any, binary bool) (*mysql.Result, error) {
	r, err := mysql.BuildSimpleResultset([]string{name}, [][]any{{val}}, binary)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(r), nil
}

// buildVariablesResultset 应答 select @@a as x, @@b 形式的系统变量查询
func (h *MyHandler) buildVariablesResultset(lower string, binary bool) (*mysql.Result, error) {
	q := strings.TrimSuffix(lower, ";")
	ps := strings.Split(q, ",")
	fields := make([]string, 0, len(ps))
	values := make([]any, 0, len(ps))
	for _, v := range ps {
		v = strings.TrimSpace(v)
		i := strings.Index(v, "@@")
		if i < 0 {
			logkit.Error(h.Title + " not exist @@: " + v)
			continue
		}
		v = v[i:]
		alias := ""
		if asIdx := strings.Index(v, " as "); asIdx >= 0 {
			alias = strings.TrimSpace(v[asIdx+len(" as "):])
			v = strings.TrimSpace(v[:asIdx])
		}
		key := normalizeSessionVarKey(v)
		val, ok := sessionVars[key]
		if !ok {
			logkit.Error(h.Title + " unsupported session variable: " + v)
			val = ""
		}
		if alias == "" {
			alias = v
		}
		fields = append(fields, alias)
		values = append(values, val)
	}
	if len(fields) == 0 {
		return nil, errors.New("no fields found")
	}
	r, err := mysql.BuildSimpleResultset(fields, [][]any{values}, binary)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(r), nil
}

func normalizeSessionVarKey(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	v = strings.TrimPrefix(v, "@@session.")
	v = strings.TrimPrefix(v, "@@global.")
	v = strings.TrimPrefix(v, "@@")
	return strings.TrimSpace(v)
}

// ---------- insert 自增主键 ----------

var reInsertTable = regexp.MustCompile(`(?is)^insert\s+(?:(?:ignore|low_priority|delayed|high_priority)\s+)*into\s+["` + "`" + `]?([A-Za-z0-9_$.]+)["` + "`" + `]?`)

// handleInsert 执行 insert；对有自增主键的表追加 RETURNING 以回填 insert_id，
// 兼容 MySQL 的 LAST_INSERT_ID()/getGeneratedKeys 语义（多行 insert 取第一个 id）
func (h *MyHandler) handleInsert(querySql, lower string, args []any) (*mysql.Result, error) {
	rewritten := h.replacePlaceholders(querySql)
	pk := ""
	if table := extractInsertTable(lower); table != "" {
		pk = h.autoIncrementColumn(table)
	}
	if pk != "" && !strings.Contains(lower, "returning") {
		rows, err := h.Target.DBPool.Queryx(rewritten+" RETURNING "+quoteIdent(pk), args...)
		if err == nil {
			return h.buildInsertReturningResult(rows)
		}
		logkit.Error(h.Title + " insert with returning error, fallback: " + err.Error())
	}
	r, err := h.Target.DBPool.Exec(rewritten, args...)
	if err != nil {
		logkit.Error(h.Title + " exec error 【" + querySql + "】" + err.Error())
		return nil, err
	}
	return h.fillExecResult(r)
}

// buildInsertReturningResult 读取 RETURNING 结果：第一行为自增 id，行数为影响行数
func (h *MyHandler) buildInsertReturningResult(rows *sqlx.Rows) (*mysql.Result, error) {
	defer rows.Close()
	res := mysql.NewResultReserveResultset(0)
	first := true
	for rows.Next() {
		ret, err := rows.SliceScan()
		if err != nil {
			return nil, err
		}
		res.AffectedRows++
		if first {
			if len(ret) > 0 {
				if id, ok := parseIDValue(ret[0]); ok {
					res.InsertId = id
					h.state.lastInsertID = id
				}
			}
			first = false
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return res, nil
}

func extractInsertTable(lower string) string {
	m := reInsertTable.FindStringSubmatch(lower)
	if len(m) < 2 {
		return ""
	}
	// 去掉 schema 前缀（db.table），主键只按表名查询
	if i := strings.LastIndex(m[1], "."); i >= 0 {
		return m[1][i+1:]
	}
	return m[1]
}

// autoIncrementColumn 查询表的自增主键列（session 内缓存，失败则停用）
func (h *MyHandler) autoIncrementColumn(table string) string {
	if h.state.pkCache == nil {
		h.state.pkCache = map[string]string{}
	}
	if h.state.pkDisabled {
		return ""
	}
	if col, ok := h.state.pkCache[table]; ok {
		return col
	}
	var col string
	// 金仓的兼容实现里 information_schema 列名不一定齐全，逐级降级
	err := h.Target.DBPool.QueryRow(
		`SELECT column_name FROM information_schema.columns
		 WHERE table_schema = current_schema() AND lower(table_name) = lower($1)
		   AND (column_default LIKE 'nextval%' OR is_identity = 'YES' OR upper(extra) = 'AUTO_INCREMENT')
		 ORDER BY ordinal_position LIMIT 1`, table).Scan(&col)
	if err != nil {
		err2 := h.Target.DBPool.QueryRow(
			`SELECT column_name FROM information_schema.columns
			 WHERE table_schema = current_schema() AND lower(table_name) = lower($1)
			   AND column_default LIKE 'nextval%'
			 ORDER BY ordinal_position LIMIT 1`, table).Scan(&col)
		if err2 != nil {
			if err2 == sql.ErrNoRows {
				// 该表没有自增主键，缓存空结果
				h.state.pkCache[table] = ""
				return ""
			}
			// information_schema 查询不可用，整体停用并只记一次日志
			h.state.pkDisabled = true
			logkit.Error(h.Title + " auto-increment column lookup disabled: " + err2.Error())
			return ""
		}
	}
	h.state.pkCache[table] = col
	return col
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func parseIDValue(v any) (uint64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case int64:
		if x >= 0 {
			return uint64(x), true
		}
	case int32:
		if x >= 0 {
			return uint64(x), true
		}
	case int:
		if x >= 0 {
			return uint64(x), true
		}
	case []byte:
		if id, err := strconv.ParseUint(strings.TrimSpace(string(x)), 10, 64); err == nil {
			return id, true
		}
	case string:
		if id, err := strconv.ParseUint(strings.TrimSpace(x), 10, 64); err == nil {
			return id, true
		}
	}
	return 0, false
}

// ---------- SQL 文本处理 ----------

// normalizeSQL 去掉开头注释与结尾分号，返回原文与小写形式
func normalizeSQL(query string) (string, string) {
	querySql := stripLeadingComments(query)
	querySql = strings.TrimSpace(querySql)
	for strings.HasSuffix(querySql, ";") {
		querySql = strings.TrimSpace(strings.TrimSuffix(querySql, ";"))
	}
	return querySql, strings.ToLower(querySql)
}

// stripLeadingComments 去掉 SQL 最前部的空白、块注释与行注释
func stripLeadingComments(query string) string {
	for {
		query = strings.TrimLeft(query, " \t\r\n")
		if strings.HasPrefix(query, "/*") {
			end := strings.Index(query, "*/")
			if end < 0 {
				return ""
			}
			query = query[end+2:]
			continue
		}
		if strings.HasPrefix(query, "--") || strings.HasPrefix(query, "#") {
			idx := strings.IndexAny(query, "\r\n")
			if idx < 0 {
				return ""
			}
			query = query[idx+1:]
			continue
		}
		return query
	}
}

// scanPlaceholders 扫描占位符 ?，跳过字符串/标识符引号与注释。
// repl 为 nil 时仅计数，否则用 repl(n) 的返回值替换第 n 个占位符。
// 返回处理后的 SQL 与占位符个数。
func scanPlaceholders(query string, repl func(n int) string) (string, int) {
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	i := 0
	for i < len(query) {
		c := query[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			j := scanQuoted(query, i)
			b.WriteString(query[i:j])
			i = j
		case c == '-' && i+1 < len(query) && query[i+1] == '-':
			j := strings.IndexByte(query[i:], '\n')
			if j < 0 {
				j = len(query) - i
			} else {
				j++ // 含换行
			}
			b.WriteString(query[i : i+j])
			i += j
		case c == '#':
			j := strings.IndexByte(query[i:], '\n')
			if j < 0 {
				j = len(query) - i
			} else {
				j++
			}
			b.WriteString(query[i : i+j])
			i += j
		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			j := strings.Index(query[i+2:], "*/")
			if j < 0 {
				j = len(query) - i
			} else {
				j += 4 // 含 */
			}
			b.WriteString(query[i : i+j])
			i += j
		case c == '?':
			n++
			if repl != nil {
				b.WriteString(repl(n))
			} else {
				b.WriteByte(c)
			}
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), n
}

// scanQuoted 返回从 i（引号处）开始的引号字面量的结束下标（不含）
func scanQuoted(query string, i int) int {
	quote := query[i]
	j := i + 1
	for j < len(query) {
		if query[j] == '\\' && quote != '`' && j+1 < len(query) {
			j += 2 // 反斜杠转义
			continue
		}
		if query[j] == quote {
			if j+1 < len(query) && query[j+1] == quote {
				j += 2 // 引号 doubled 转义
				continue
			}
			return j + 1
		}
		j++
	}
	return len(query)
}

// replacePlaceholders 把 ? 替换为金仓/PG 风格的 $N
func (h *MyHandler) replacePlaceholders(query string) string {
	switch h.Target.Driver {
	case DriverKingbase, DriverPostgres:
		out, _ := scanPlaceholders(query, func(n int) string { return "$" + strconv.Itoa(n) })
		return out
	default:
		return query
	}
}

func countPlaceholders(query string) int {
	_, n := scanPlaceholders(query, nil)
	return n
}

// convertArgs 把 go-mysql 解析出的参数转成金仓驱动支持的范围
func convertArgs(args []any) []any {
	if len(args) == 0 {
		return args
	}
	for i, a := range args {
		switch v := a.(type) {
		case uint64:
			if v > math.MaxInt64 {
				args[i] = cast.ToString(v)
			} else {
				args[i] = int64(v)
			}
		case uint32:
			args[i] = int64(v)
		case uint16:
			args[i] = int64(v)
		case uint8:
			args[i] = int64(v)
		case uint:
			args[i] = int64(v)
		case float32:
			args[i] = float64(v)
		}
	}
	return args
}

func (h *MyHandler) convertGoValueToMysqlValue(val any) any {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case bool:
		return cast.ToInt8(v)
	default:
		return val
	}
}
