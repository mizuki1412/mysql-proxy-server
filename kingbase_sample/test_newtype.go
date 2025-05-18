package main

import (
	"database/sql"
	"fmt"
	_ "kingbase.com/gokb"
)

const (
	host     = "192.168.28.36"
	port     = 54333
	user     = "system"
	password = "123456"

	dbname = "test"
)

// table struct
type GoTable struct {
	Num   int    `db:"num"`
	Bcb   string `db:"bcb"`
	Vcb   string `db:"vcb"`
	Date  string `db:"date"`
	Blob  []byte `db:"blob"`
	Nclob string `db:"nclob"`
}

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

func testInsert(db *sql.DB, sql string) (err error) {
	err = testTran(db, sql)
	return err
}

func testPrepare(db *sql.DB, sql string) (err error) {
	stmt, err := db.Prepare(sql)
	defer stmt.Close()
	if err != nil {
		return err
	}
	_, err = stmt.Exec(int64(100), "bpcharbyte 中文示例文字", "varcharbyte 中文示例文字", "2020-01-01", []byte{10, 20, 30, 40, 50}, "NCLOB中文示例文字")
	if err != nil {
		return err
	}
	return nil
}

func testSelect(db *sql.DB, query string) (data []*GoTable, err error) {
	rows, err := db.Query(query)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var row GoTable
		err = rows.Scan(&row.Num, &row.Bcb, &row.Vcb, &row.Date, &row.Blob, &row.Nclob)
		if err != nil {
			return nil, err
		}

		data = append(data, &row)
	}
	return data, nil
}

func main() {
	connInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := testConn(connInfo)
	if err != nil {
		panic(err)
	}
	fmt.Println("Connection test success!")
	defer db.Close()

	err = testTran(db, "CREATE temp TABLE temp_golang_test (num INTEGER, bcb bpcharbyte(30), vcb varcharbyte(30), date date, blob BLOB, nclob NCLOB)")
	if err != nil {
		panic(err)
	}
	fmt.Println("Transaction test success!")

	err = testInsert(db, "insert into temp_golang_test(num,bcb,vcb,date,blob,nclob) values"+
		"(123456,'ABCDEF123456','abcdef123456','2020-02-29 12:34:56+08','123456!@#$%^','ABC“”『』【】《》');")
	if err != nil {
		panic(err)
	}
	fmt.Println("Insert data test success!")

	err = testPrepare(db, "insert into temp_golang_test values($1,$2,$3,$4,$5,$6)")
	if err != nil {
		panic(err)
	}
	fmt.Println("Prepare and execute test success!")

	data, err := testSelect(db, "select * from temp_golang_test;")
	if err != nil {
		panic(err)
	}

	fmt.Println(data[0])
	fmt.Println(data[1])
	fmt.Println("Select test success!")

}
