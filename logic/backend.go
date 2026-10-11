package logic

import (
	"fmt"
	"github.com/example/go-frame/pkg/class/exception"
	c2 "github.com/example/go-frame/pkg/library/c"
	"github.com/example/go-frame/pkg/service/logkit"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"net"
)

var index = 1

func Main() {
	for _, e := range ConfigBean.Pair {
		c2.RecoverGoFuncWrapper(func() {
			l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", e.Host.Port))
			if err != nil {
				panic(exception.New(err.Error()))
			}

			for {
				// Accept a new connection once
				c, err := l.Accept()
				if err != nil {
					logkit.Error(err.Error())
					continue
				}
				c2.RecoverGoFuncWrapper(func() {
					handleConn(c, e.Host, *e.Target)
				})
			}
		})
	}
	select {}
}

// 处理一个连接一个客户端
func handleConn(c net.Conn, host *Host, target Target) {
	title := fmt.Sprintf("【%d-%s】【%d-%s】", host.Port, target.Db, index, c.RemoteAddr().(*net.TCPAddr).IP)
	target = OpenDBConnection(title, target)
	defer target.DBPool.Close()
	authHandler := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if err := authHandler.AddUser(host.Username, host.Password); err != nil {
		panic(exception.New(err.Error()))
	}
	conn, err := server.NewServer(
		mysqlServerVersion,
		33, // utf8，对php的pdo适配，  mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD,
		nil, nil).NewCustomizedConn(c, authHandler, &MyHandler{
		Target: target,
		Title:  title,
		state:  &sessionState{},
	})
	if err != nil {
		panic(exception.New(err.Error()))
	}
	index++
	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			// 出错时 go-mysql 已关闭底层连接，正常退出（QUIT）不视为异常
			if conn.Conn == nil {
				logkit.Debug(title + " client connection closed")
			} else {
				logkit.Error(title + " command error: " + err.Error())
			}
			return
		}
	}
}
