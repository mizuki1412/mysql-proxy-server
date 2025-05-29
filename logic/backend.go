package logic

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	"log"
	"net"
)

func Main() {
	// 先根据config初始化双方数据库连接
	InitConfigConnection()

	l, err := net.Listen("tcp", "0.0.0.0:24000")
	if err != nil {
		log.Fatal(err)
	}

	for {
		// Accept a new connection once
		c, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConn(c)
	}
}

// 处理一个连接一个客户端
func handleConn(c net.Conn) {
	p := server.NewInMemoryProvider()
	p.AddUser("root", "123")
	conn, err := server.NewServer(
		"8.0.11",
		mysql.DEFAULT_COLLATION_ID,
		mysql.AUTH_NATIVE_PASSWORD,
		nil, nil).NewCustomizedConn(c, p, MyHandler{})
	if err != nil {
		log.Fatal(err)
	}

	// as long as the client keeps sending commands, keep handling them
	for {
		if err := conn.HandleCommand(); err != nil {
			log.Println(err)
			break
		}
	}
}
