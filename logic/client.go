package logic

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
)

func ClientExample() {
	// Connect MySQL at 127.0.0.1:3306, with user root, an empty password and database test
	conn, err := client.Connect("127.0.0.1:24000", "root", "123", "myspace")

	err = conn.Ping()
	if err != nil {
		panic(err)
	}

	// Insert
	//r, err := conn.Execute(`INSERT INTO sys_user(role, username) VALUES(1, ?) returning id`, "test2")
	//if err != nil {
	//	panic(err)
	//}
	//defer r.Close()
	//// Get last insert id and number of affected rows
	//fmt.Printf("InsertId: %d, AffectedRows: %d\n", r.InsertId, r.AffectedRows)
	//
	//r, err = conn.Execute(`update sys_user set name=? where username='test1'`, "test222")
	//if err != nil {
	//	panic(err)
	//}
	//defer r.Close()
	//// Get last insert id and number of affected rows
	//fmt.Printf("InsertId: %d, AffectedRows: %d\n", r.InsertId, r.AffectedRows)

	// Select
	r, err := conn.Execute(`SELECT * FROM sys_user where id>?`, 1)
	if err != nil {
		if err, ok := err.(interface{ StackTrace() errors.StackTrace }); ok {
			fmt.Sprintf("%+v", err.StackTrace())
		}
		panic(err)
	}

	// Handle resultset
	//v, err := r.GetInt(0, 0)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("Value of Row 0, Column 0: %d\n", v)

	//v, err = r.GetIntByName(0, "id")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Printf("Value of Row 0, Column 'id': %d\n", v)

	// Direct access to fields
	for rownum, row := range r.Values {
		fmt.Printf("Row number %d\n", rownum)
		for colnum, val := range row {
			fmt.Printf("\tColumn number %d\n", colnum)

			//ival := val.Value() // interface{}
			//fmt.Printf("\t\tvalue (type: %d): %#v\n", val.Type, ival)

			if val.Type == mysql.FieldValueTypeSigned {
				fval := val.AsInt64()
				fmt.Printf("\t\tint64 value: %d\n", fval)
			}
			if val.Type == mysql.FieldValueTypeString {
				fval := val.AsString()
				fmt.Printf("\t\tstring value: %s\n", fval)
			}
		}
	}
}
