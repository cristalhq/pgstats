package pgstats

import (
	"database/sql"
	"fmt"
)

func Example() {
	var db *sql.DB
	// init db

	stats, err := New(db)
	if err != nil {
		// ...
	}

	all, err := stats.AllIndexes()
	if err != nil {
		// ...
	}

	for _, index := range all {
		fmt.Printf("index name: %v\n", index.Indexrelname)
	}
}
