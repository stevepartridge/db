package db

import (
	"database/sql"
	// "errors"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/propervillains/log"
)

type Database struct {
	Type    string
	Id      string
	Host    string
	Port    string
	Name    string
	User    string
	Pass    string
	SSLMode string // postgres
	conn    *sql.DB
	Mock    sqlmock.Sqlmock
}

var (
	databases []Database
)

func (db *Database) Connect() *sql.DB {

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

	log.IfError(err)

	if err == nil {
		err = db.conn.Ping()
		log.IfError(err)
	}

	return db.conn

}

func Get(id string) *Database {

	for i := range databases {
		if databases[i].Id == id {
			return &databases[i]
		}
	}

	log.Warnf("Unable to find database by id: %d", id)

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
				log.Warnf("Error pinging db: %s", err)
				if err.Error() == "sql: database is closed" {
					log.Notice("will attempt to reconnect")
				}
				return d.Connect()
			}

			return d.conn
		}
		return d.Connect()
	}
	return nil
}

func Check(id string) error {
	conn := Conn(id)

	rows, err := conn.Query(`SELECT NOW()`)
	defer rows.Close()
	log.IfError(err)

	if err == nil {
		log.Notice("Successfully connected to DB ID:", id)
	}

	return err

}

func Add(db Database) {

	for i := range databases {
		if db.Id == databases[i].Id {
			log.Warnf("%s exists, overriding with new db", db.Id)
			databases[i] = db
			return
		}
	}

	_ = db.Connect()

	databases = append(databases, db)
	log.Info("db.Add", db.Type, db.Id)
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
