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
	Targets  []*Target `json:"targets"`
	Port     int       `json:"port"`
	Username string    `json:"username"`
	Password string    `json:"password"`
}

type Target struct {
	Src  *TargetDetail `json:"src"`
	Dist *TargetDetail `json:"dist"`
}

type TargetDetail struct {
	Driver   string   `json:"driver"`
	Db       string   `json:"db"`
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

func (th DBConfig) GetTargetDetailBySrc(src string) *TargetDetail {
	for _, e := range th.Targets {
		if e.Src != nil && e.Src.Db == src {
			return e.Dist
		}
	}
	return nil
}

func InitConfigConnection() {
	for _, e := range ConfigBean.Targets {
		openDBConnection(e.Dist)
	}
}

func openDBConnection(target *TargetDetail) {
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
