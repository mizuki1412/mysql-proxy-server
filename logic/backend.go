package logic

import (
	"github.com/go-mysql-org/go-mysql/server"
	"log"
	"net"
)

func Main() {
	// 先根据config初始化双方数据库连接
	InitConfigConnection()

	//for _, e := range ConfigBean.Targets {
	//	// 每个target是一个服务
	//	go func() {
	//
	//	}()
	//	select {}
	//}

}

func Test() {
	l, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		log.Fatal(err)
	}

	// Accept a new connection once
	c, err := l.Accept()
	if err != nil {
		log.Fatal(err)
	}

	// Create a connection with user root and an empty password.
	// You can use your own handler to handle command here.
	conn, err := server.NewDefaultServer().NewConn(c, "root", "", server.EmptyHandler{})
	if err != nil {
		log.Fatal(err)
	}

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			log.Fatal(err)
		}
	}
}
