package logic

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/mizuki1412/go-core-kit/v2/class/exception"
	"github.com/mizuki1412/go-core-kit/v2/library/jsonkit"
	"github.com/mizuki1412/go-core-kit/v2/service/logkit"
	"log/slog"
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
	Ip       string   `json:"ip"`
	Port     int      `json:"port"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	DBPool   *sqlx.DB `json:"-"`
}

const (
	DriverMysql    = "mysql"
	DriverKingbase = "kingbase"
	DriverPostgres = "postgres"
)

var ConfigBean = &DBConfig{}

//func InitConfigConnection() {
//	for _, e := range ConfigBean.Pair {
//		openDBConnection(e.Target)
//	}
//}

func OpenDBConnection(target *Target) {
	if target.Driver == DriverKingbase || target.Driver == DriverPostgres {
		connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			target.Ip, target.Port, target.Username, target.Password, target.Db)
		db, err := sqlx.Open(target.Driver, connInfo)
		if err != nil {
			panic(exception.New("数据库连接失败: " + jsonkit.ToString(target)))
		}
		err = db.Ping()
		if err != nil {
			panic(exception.New("数据库ping失败: " + jsonkit.ToString(target)))
		}
		target.DBPool = db
		logkit.Info("数据库连接成功", slog.String("db", connInfo))
	}
}
