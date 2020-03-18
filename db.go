package db

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Database struct {
	Type         string
	Id           string
	Host         string
	Port         string
	Name         string
	User         string
	Pass         string
	SSLMode      string // postgres
	conn         *sql.DB
	connx        *sqlx.DB
	Mock         sqlmock.Sqlmock
	MaxOpenConns int
	MaxIdleConns int

	retries int
}

var (
	MaxRetries = 0 // Infinite retries

	databases []Database
)

func (db *Database) Connect(retry ...bool) *sql.DB {

	if len(retry) > 0 && db.retries > 0 {
		fmt.Printf("(%d) Attempting retry...\n", db.retries)
	}

	if db.MaxOpenConns == 0 {
		db.MaxOpenConns = 25
	}

	if db.MaxIdleConns == 0 {
		db.MaxIdleConns = 25
	}

	var (
		host = db.Host
		err  error
	)
	if db.Port != "" {
		host = host + ":" + db.Port
	}

	switch db.Type {

	case "mysql":
		db.conn, err = sql.Open("mysql", fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?multiStatements=true",
			db.User,
			db.Pass,
			host,
			db.Name,
		))
		db.conn.SetMaxOpenConns(db.MaxOpenConns)
		db.conn.SetMaxIdleConns(db.MaxIdleConns)

	case "postgres":
		db.conn, err = sql.Open("postgres", fmt.Sprintf(
			"host=%s dbname=%s user=%s password='%s' port=%s sslmode=%s",
			db.Host,
			db.Name,
			db.User,
			db.Pass,
			db.Port,
			db.SSLMode,
		))

	case "mock":
		db.conn, db.Mock, err = sqlmock.New()

	}
	db.conn.SetMaxOpenConns(db.MaxOpenConns)
	db.conn.SetMaxIdleConns(db.MaxIdleConns)

	ifError(err)

	if err == nil {
		err = db.conn.Ping()
		if ifError(err) {
			db.retries++
			if len(retry) > 0 {
				if MaxRetries > 0 {
					if db.retries > MaxRetries {
						fmt.Printf("Reached max retries (%d), returning nil")
						return nil
					}
				}
			}
			time.Sleep(2 * time.Second)
			return db.Connect(true)
		}
		fmt.Printf("connected to %s\n", db.Id)
	}

	return db.conn

}

func Get(id string) *Database {

	for i := range databases {
		if databases[i].Id == id {
			return &databases[i]
		}
	}

	fmt.Printf("Unable to find database by id: %d\n", id)

	return nil
}

func Conn(id string) *sql.DB {
	d := Get(id)
	if d != nil {
		if d.conn != nil {

			// TODO:
			// 		move this to a proactive approach,
			// 		as not to avoid adding it to the
			// 		overhead/latency of the call
			//
			if err := d.conn.Ping(); err != nil {
				fmt.Printf("Error pinging db: %s\n", err)
				if err.Error() == "sql: database is closed" {
					fmt.Printf("will attempt to reconnect\n")
				}
				return d.Connect()
			}

			return d.conn
		}
		return d.Connect()
	}
	return nil
}

func Connx(id string) *sqlx.DB {

	d := Get(id)
	if d != nil {

		if d.connx == nil {
			if d.conn == nil {
				d.Connect()
			}

			d.connx = sqlx.NewDb(d.conn, d.Type)
			for i := range databases {
				if databases[i].Id == id {
					databases[i] = *d
				}
			}
			return d.connx
		}

		// TODO:
		// 		move this to a proactive approach,
		// 		as not to avoid adding it to the
		// 		overhead/latency of the call
		//
		if err := d.connx.Ping(); err != nil {
			fmt.Printf("Error pinging db: %s\n", err)
			if err.Error() == "sql: database is closed" {
				fmt.Printf("will attempt to reconnect\n")
			}
			d.connx = sqlx.NewDb(d.conn, d.Type)

			return d.connx
		}
	}
	return nil

}

func Check(id string) error {
	conn := Conn(id)

	if conn == nil {
		return errors.New("Connection is nil")
	}

	err := conn.Ping()
	if err != nil {
		ifError(err)
		return err
	}

	rows, err := conn.Query(`SELECT NOW()`)
	defer rows.Close()
	ifError(err)

	if err == nil {
		fmt.Printf("Successfully connected to DB %s\n", id)
	}

	return err

}

func Add(db Database) {
	// make sure to always connect the database first
	_ = db.Connect()

	// check if database has already been added with the same ID
	for i := range databases {
		if db.Id == databases[i].Id {
			fmt.Printf("%s exists, overriding with new db\n", db.Id)
			databases[i] = db
			return
		}
	}

	// append database since ID was not found
	databases = append(databases, db)
	fmt.Printf("added %s database: %s\n", db.Type, db.Id)
}

func AddMySQL(id, host, port, name, user, pass string) {
	Add(Database{
		Type: "mysql",
		Id:   id,
		Host: host,
		Port: port,
		Name: name,
		User: user,
		Pass: pass,
	})
}

func AddPostgres(id, host, port, name, user, pass, sslMode string) {
	Add(Database{
		Type:    "postgres",
		Id:      id,
		Host:    host,
		Port:    port,
		Name:    name,
		User:    user,
		Pass:    pass,
		SSLMode: sslMode,
	})
}

func AddMock(id string) {
	Add(Database{
		Type: "mock",
		Id:   id,
	})
}

func ifError(err error) bool {
	if err != nil {
		fmt.Println("Error (db):", err.Error())
		return true
	}
	return false
}
