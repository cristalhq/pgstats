package pgstats

import "database/sql"

// Archiver returns rows from a `pg_stat_archiver` view.
// One row only, showing statistics about the WAL archiver process's activity. See pg_stat_archiver for details.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ARCHIVER-VIEW
func (s *Stats) Archiver() (ArchiverView, error) {
	return s.fetchArchiver()
}

// ArchiverView represents content of pg_stat_archiver view
type ArchiverView struct {
	ArchivedCount    *sql.NullInt64  `json:"archived_count"`     // Number of WAL files that have been successfully archived
	LastArchivedWal  *sql.NullString `json:"last_archived_wal"`  // Name of the last WAL file successfully archived
	LastArchivedTime *sql.NullTime   `json:"last_archived_time"` // Time of the last successful archive operation
	FailedCount      *sql.NullInt64  `json:"failed_count"`       // Number of failed attempts for archiving WAL files
	LastFailedWal    *sql.NullString `json:"last_failed_wal"`    // Name of the WAL file of the last failed archival operation
	LastFailedTime   *sql.NullTime   `json:"last_failed_time"`   // Time of the last failed archival operation
	StatsReset       *sql.NullTime   `json:"stats_reset"`        // Time at which these statistics were last reset
}

func (s *Stats) fetchArchiver() (ArchiverView, error) {
	const query = `SELECT
	archived_count,
	last_archived_wal,
	last_archived_time,
	failed_count,
	last_failed_wal,
	last_failed_time,
	stats_reset
	FROM pg_stat_archiver`

	row := s.db.QueryRow(query)
	var res ArchiverView

	err := row.Scan(
		&res.ArchivedCount,
		&res.LastArchivedWal,
		&res.LastArchivedTime,
		&res.FailedCount,
		&res.LastFailedWal,
		&res.LastFailedTime,
		&res.StatsReset,
	)
	return res, err
}
