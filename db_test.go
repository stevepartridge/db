package db

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func Test_Unit_AddDbSuccess(t *testing.T) {
	AddMock("test")
	db := Get("test")

	if db == nil {
		t.Error(errors.New("db should not be nil"))
	}
}

func Test_Unit_GetNilDb(t *testing.T) {
	db := Get("test-nil")

	if db != nil {
		t.Error(errors.New("db should be nil"))
	}
}

func Test_Unit_ConnNilDb(t *testing.T) {
	db := Conn("test-nil2")

	if db != nil {
		t.Error(errors.New("db should be nil"))
	}
}

func Test_Unit_AddDupilicateDbSuccess(t *testing.T) {
	AddMock("test")
	AddMock("test")
	d := Get("test")

	if d == nil {
		t.Error(errors.New("db should not be nil"))
	}
}

func Test_Unit_AddMySqlDbSuccess(t *testing.T) {
	AddMySQL("test-mysql", "host", "1234", "name", "user", "pass")

	d := Get("test-mysql")

	if d == nil {
		t.Error(errors.New("db should not be nil"))
	}
}
func Test_Unit_AddPostgresDbSuccess(t *testing.T) {
	AddPostgres("test-pg", "host", "1234", "name", "user", "pass", "disable")

	d := Get("test-pg")

	if d == nil {
		t.Error(errors.New("db should not be nil"))
	}
}

func Test_Unit_CheckDbSuccess(t *testing.T) {

	AddMock("test-1")

	_db := Get("test-1")

	if _db == nil {
		t.Error("_db is nil")
	}

	rows := sqlmock.NewRows([]string{"NOW()"}).
		AddRow(time.Time{})

	_db.Mock.ExpectQuery(`^SELECT (.+)`).
		WillReturnRows(rows)

	err := Check("test-1")
	if err != nil {
		t.Error("unexpected error %v", err)
	}

	if err := _db.Mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expections: %s", err)
	}
}
