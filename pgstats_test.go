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
	flag.StringVar(&host, "host", "127.0.0.1", "test host")
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

	fmt.Printf("test connection: %v\n\n", connString)

	var err error
	testConn, err = sql.Open("postgres", connString)
	if err != nil {
		panic(err)
	}
	if err := testConn.Ping(); err != nil {
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

func TestActivity(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Activity()
	isOK(t, 1, err)
}

func TestArchiver(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Archiver()
	isOK(t, 1, err)
}

func TestBgWriter(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.BgWriter()
	isOK(t, 1, err)
}

func TestDatabaseConflicts(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.DatabaseConflicts()
	isOK(t, 1, err)
}

func TestDatabase(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Database()
	isOK(t, 1, err)
}

func TestFunctions(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	funcs, err := stats.UserFunctions()
	isOK(t, len(funcs), err)

	xfuncs, err := stats.XactUserFunctions()
	isOK(t, len(xfuncs), err)
}

func TestIndex(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	all, err := stats.AllIndexes()
	isOK(t, len(all), err)

	sys, err := stats.SystemIndexes()
	isOK(t, len(sys), err)

	usr, err := stats.UserIndexes()
	isOK(t, len(usr), err)
}

func TestIoIndex(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	all, err := stats.IoAllIndexes()
	isOK(t, len(all), err)

	sys, err := stats.IoSystemIndexes()
	isOK(t, len(sys), err)

	usr, err := stats.IoUserIndexes()
	isOK(t, len(usr), err)
}

func TestProgressVacuum(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.ProgressVacuum()
	isOK(t, 1, err)
}

func TestReplication(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Replication()
	isOK(t, 1, err)
}

func TestSequences(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	all, err := stats.IoAllSequences()
	isOK(t, len(all), err)

	sys, err := stats.IoSystemSequences()
	isOK(t, len(sys), err)

	usr, err := stats.IoSystemSequences()
	isOK(t, len(usr), err)
}

func TestSsl(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Ssl()
	isOK(t, 1, err)
}

func TestStatements(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Statements()
	isOK(t, 1, err)
}

func TestSubscription(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.Subscription()
	isOK(t, 1, err)
}

func TestTables(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	all, err := stats.AllTables()
	isOK(t, len(all), err)

	sys, err := stats.SystemTables()
	isOK(t, len(sys), err)

	usr, err := stats.UserTables()
	isOK(t, len(usr), err)
}

func TestWalReceiver(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	_, err = stats.WalReceiver()
	isOK(t, 1, err)
}

func TestXactTables(t *testing.T) {
	stats, err := New(testConn)
	noErr(t, err)

	all, err := stats.XactAllTables()
	isOK(t, len(all), err)

	sys, err := stats.XactSystemTables()
	isOK(t, len(sys), err)

	usr, err := stats.XactUserTables()
	isOK(t, len(usr), err)
}
