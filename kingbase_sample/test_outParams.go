package main

import (
	"database/sql"
	"fmt"
	"kingbase.com/gokb"
	_ "kingbase.com/gokb"
)

const (
	host     = "127.0.0.1"
	port     = 54321
	user     = "system"
	password = "123456"
	dbname   = "TEST"
)

func testConn(connInfo string) (db *sql.DB, err error) {
	db, err = sql.Open("kingbase", connInfo)
	if err != nil {
		return db, err
	}

	err = db.Ping()
	if err != nil {
		return db, err
	}
	return db, err
}

func testTran(db *sql.DB, sql string) (err error) {
	txn, err := db.Begin()
	defer txn.Commit()
	if err != nil {
		return err
	}

	_, err = txn.Exec(sql)
	if err != nil {
		return err
	}
	return nil
}

type GoTable struct {
	Num1 int    `db:"num"`
	Name string `db:"varchar"`
}

func testOutCursor(db *sql.DB) (err error) {
	err = testTran(db, "drop table if exists testCursor;"+
		"create table testCursor(id int, name varchar(100));")
	if err != nil {
		panic(err)
	}

	err = testTran(db, "insert into testCursor values(1, 'zhangsan'),(2, 'lisi'),(3, 'wangwu');")
	if err != nil {
		panic(err)
	}

	err = testTran(db, "drop procedure if exists test_out_cursor;"+
		"CREATE or replace procedure test_out_cursor(i in int, c out refcursor) as "+
		"declare c1 refcursor; begin open c1 for select name from testCursor where id = i;c := c1; end")
	if err != nil {
		panic(err)
	}

	txn, err := db.Begin()
	defer txn.Commit()
	if err != nil {
		return err
	}
	//调用存储过程的预备语句
	preparedStmt, err := txn.Prepare("call test_out_cursor(:inarg, :outarg);")
	defer preparedStmt.Close()
	if err != nil {
		return err
	}
	//调用存储过程获取游标名
	var count1 int = 1
	var count2 gokb.CursorString
	params := make([]interface{}, 2)
	params[0] = sql.Named("inarg", count1)
	params[1] = sql.Named("outarg", sql.Out{Dest: &count2})
	_, err = preparedStmt.Exec(params...)
	if err != nil {
		return err
	}
	fmt.Println("cursor name is:", count2.CursorName)
	sqlString := fmt.Sprintf("FETCH ALL IN \"%s\";", count2.CursorName)
	rows, err := txn.Query(sqlString)
	if err != nil {
		return err
	}
	var data []*GoTable
	for rows.Next() {
		var row GoTable
		err = rows.Scan(&row.Name)
		if err != nil {
			return err
		}

		data = append(data, &row)
	}
	fmt.Println(data[0].Name)

	return nil
}

func main() {
	connInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable connect_timeout=1 ", //binary_parameters=yes
		host, port, user, password, dbname)

	db, err := testConn(connInfo)
	if err != nil {
		panic(err)
	}
	fmt.Println("Connection test success!")
	defer db.Close()

	err = testOutCursor(db)
	if err != nil {
		panic(err)
	}
	fmt.Println("outCursor test Success!")
}
