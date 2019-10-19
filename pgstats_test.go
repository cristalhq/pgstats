package pgstats

import (
	"database/sql"
	"flag"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
)

var host, port, dbname, user, pass, mode string

var testConn *sql.DB

func init() {
	flag.StringVar(&host, "host", "postgres", "test host")
	flag.StringVar(&port, "port", "5432", "test port")
	flag.StringVar(&dbname, "dbname", "postgres_db", "test db name")
	flag.StringVar(&user, "user", "postgres_user", "test username")
	flag.StringVar(&pass, "pass", "postgres_pass", "test pass")
	flag.StringVar(&mode, "mode", "disable", "test mode")

	connString := fmt.Sprintf(`
	host=%s port=%s
	user=%s password=%s
	dbname=%s
	sslmode=%s`, host, port, user, pass, dbname, mode)

	var err error
	testConn, err = sql.Open("postgres", connString)
	if err != nil {
		panic(err)
	}
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func isOK(t *testing.T, size int, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if size == 0 {
		t.Fatal("No data from query")
	}
}

func TestArchiver(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Archiver()
	isOK(t, 1, err)
}
