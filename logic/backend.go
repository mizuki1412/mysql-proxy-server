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
					handleConn(c, e)
				})
			}
		})
	}
	select {}
}

// 处理一个连接一个客户端
func handleConn(c net.Conn, config *Pair) {
	OpenDBConnection(config.Target)
	defer config.Target.DBPool.Close()
	p := server.NewInMemoryProvider()
	p.AddUser(config.Host.Username, config.Host.Password)
	conn, err := server.NewServer(
		"8.0.11",
		mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD,
		nil, nil).NewCustomizedConn(c, p, MyHandler{
		Target: config.Target,
		Title:  fmt.Sprintf("【%d-%s】【%d-%s】", config.Host.Port, config.Target.Db, index, c.RemoteAddr().(*net.TCPAddr).IP)})
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
