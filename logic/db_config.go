package logic

import (
	"fmt"
	"log/slog"

	"github.com/example/go-frame/pkg/class/exception"
	"github.com/example/go-frame/pkg/library/jsonkit"
	"github.com/example/go-frame/pkg/service/logkit"
	"github.com/jmoiron/sqlx"
)

type DBConfig struct {
	Pair []*Pair `json:"pair"`
}

type Pair struct {
	Host   *Host   `json:"host"`
	Target *Target `json:"target"`
}

type Host struct {
	Port     int    `json:"port"` // 监听的端口
	Username string `json:"username"`
	Password string `json:"password"`
}

type Target struct {
	Driver   string   `json:"driver"`
	Db       string   `json:"db"` // db name
	Schema   string   `json:"schema"`
	Timezone string   `json:"timezone"` // 可选，如 Asia/Shanghai，影响 timestamptz 读取时区
	Ip       string   `json:"ip"`
	Port     int      `json:"port"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	DBPool   *sqlx.DB `json:"-"`
}

const (
	DriverKingbase = "kingbase"
	DriverPostgres = "postgres"
)

var ConfigBean = &DBConfig{}

func OpenDBConnection(title string, target Target) Target {
	if target.Driver == DriverKingbase || target.Driver == DriverPostgres {
		connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			target.Ip, target.Port, target.Username, target.Password, target.Db)
		if target.Schema != "" {
			// 驱动会把连接串里未识别的参数作为启动参数发给服务端
			connInfo += " search_path=" + target.Schema
		}
		if target.Timezone != "" {
			connInfo += " timezone=" + target.Timezone
		}
		db, err := sqlx.Open(target.Driver, connInfo)
		if err != nil {
			panic(exception.New(title + " 数据库连接失败: " + jsonkit.ToString(target)))
		}
		err = db.Ping()
		if err != nil {
			panic(exception.New(title + " 数据库ping失败: " + jsonkit.ToString(target)))
		}
		// 客户端连接与后端连接一对一绑定：MySQL 是"一连接一会话"语义，
		// 事务状态（begin/commit）必须始终落在同一条物理连接上，
		// 若放任连接池漂移，begin 与后续语句会落在不同连接导致事务撕裂
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		target.DBPool = db
		logkit.Info(title+" 数据库连接成功", slog.String("db", connInfo))
	}
	return target
}
