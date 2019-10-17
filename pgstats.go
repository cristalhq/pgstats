package pgstats

import (
	"database/sql"
)

// Stats provides an access to the Postgres monitoring statistics.
type Stats struct {
	db *sql.DB
}

// New creates a new Stats to access Postgres stats.
func New(db *sql.DB) (*Stats, error) {
	if err := db.Ping(); err != nil {
		return nil, err
	}
	s := &Stats{
		db: db,
	}
	return s, nil
}

// Close closes the connection to the sdatabase.
func (s *Stats) Close() error {
	return s.db.Close()
}
