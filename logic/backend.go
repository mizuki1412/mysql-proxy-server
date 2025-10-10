package logic

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/mizuki1412/go-core-kit/v2/class/exception"
	c2 "github.com/mizuki1412/go-core-kit/v2/library/c"
	"github.com/mizuki1412/go-core-kit/v2/service/logkit"
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
	p := server.NewInMemoryProvider()
	p.AddUser(host.Username, host.Password)
	conn, err := server.NewServer(
		"8.0.11",
		33, // utf8，对php的pdo适配，  mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD,
		nil, nil).NewCustomizedConn(c, p, MyHandler{Target: target, Title: title})
	if err != nil {
		panic(exception.New(err.Error()))
	}
	index++
	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			panic(exception.New(err.Error()))
		}
	}
}
