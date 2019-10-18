package pgstats

import (
	"database/sql"
	"errors"
	"regexp"
	"strconv"
)

var versionRegex = regexp.MustCompile(`(^9\.\d)|(^\d{2})`)

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

func (s *Stats) getVersion() (float64, error) {
	const query = "SHOW server_version;"
	row := s.db.QueryRow(query)

	var version string
	err := row.Scan(&version)
	if err != nil {
		return 0, err
	}
	return parseMajorVersion(version)
}

func parseMajorVersion(s string) (float64, error) {
	v := versionRegex.FindString(s)
	if v == "" {
		return 0, errors.New("Regex parse error")
	}

	return strconv.ParseFloat(v, 64)
}
