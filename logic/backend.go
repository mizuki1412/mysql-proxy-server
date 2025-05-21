package logic

import (
	"context"
	"fmt"
	sqls "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	sqle "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/mizuki1412/go-core-kit/v2/class/exception"
)

func Main() {
	// 先根据config初始化双方数据库连接
	InitConfigConnection()

	for _, e := range ConfigBean.Targets {
		// 每个target是一个服务
		go func() {
			engine := sqls.NewDefault(e.Dist.Provider)
			// 配置服务器
			config := server.Config{
				Protocol: "tcp",
				Address:  fmt.Sprintf("0.0.0.0:%d", e.Port),
			}
			// 启动服务器
			s, err := server.NewServer(config, engine, func(ctx context.Context, conn *mysql.Conn, addr string) (sqle.Session, error) {
				host := ""
				user := ""
				//mysqlConnectionUser, ok := conn.UserData.(sqle.MysqlConnectionUser)
				//if ok {
				//	host = mysqlConnectionUser.Host
				//	user = mysqlConnectionUser.User
				//}
				client := sqle.Client{Address: host, User: user, Capabilities: conn.Capabilities}
				baseSession := sqle.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID)
				return memory.NewSession(baseSession, e.Dist.Provider), nil
			}, nil)
			if err != nil {
				panic(exception.New("server new error: " + err.Error()))
			}
			if err = s.Start(); err != nil {
				panic(exception.New("server start error: " + err.Error()))
			}
		}()
		select {}
	}

}
