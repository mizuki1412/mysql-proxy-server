package logic

import (
	"database/sql"
	"fmt"
	"github.com/mizuki1412/go-core-kit/v2/class/exception"
	"github.com/mizuki1412/go-core-kit/v2/library/jsonkit"
)

var ConfigBean = &DBConfig{}

func InitConfigConnection() {
	for _, e := range ConfigBean.Targets {
		openDBConnection(e.Dist)
	}
}

func openDBConnection(target *TargetDetail) {
	if target.Driver == DriverKingbase || target.Driver == DriverPostgres {
		connInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			target.Ip, target.Port, target.Username, target.Password, target.Db)
		db, err := sql.Open(target.Driver, connInfo)
		if err != nil {
			panic(exception.New("数据库连接失败: " + jsonkit.ToString(target)))
		}
		err = db.Ping()
		if err != nil {
			panic(exception.New("数据库ping失败: " + jsonkit.ToString(target)))
		}
		target.Provider = &DBProvider{
			db:     db,
			Driver: target.Driver,
			Db:     target.Db,
			Schema: target.Schema,
		}
	}
}
