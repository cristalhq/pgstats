package pgstats

import "database/sql"

// DatabaseConflicts returns rows from a `pg_stat_database_conflicts` view.
// One row per database, showing database-wide statistics about query cancels due to conflict with recovery on standby servers.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-CONFLICTS-VIEW
func (s *Stats) DatabaseConflicts() ([]DatabaseConflictsRow, error) {
	return s.fetchDatabaseConflicts()
}

// DatabaseConflictsRow represents row from `pg_stat_database_conflicts` view.
type DatabaseConflictsRow struct {
	Datid           int64          `json:"datid"`            // OID of a database
	Datname         string         `json:"datname"`          // Name of this database
	ConflTablespace *sql.NullInt64 `json:"confl_tablespace"` // Number of queries in this database that have been canceled due to dropped tablespaces
	ConflLock       *sql.NullInt64 `json:"confl_lock"`       // Number of queries in this database that have been canceled due to lock timeouts
	ConflSnapshot   *sql.NullInt64 `json:"confl_snapshot"`   // Number of queries in this database that have been canceled due to old snapshots
	ConflBufferpin  *sql.NullInt64 `json:"confl_bufferpin"`  // Number of queries in this database that have been canceled due to pinned buffers
	ConflDeadlock   *sql.NullInt64 `json:"confl_deadlock"`   // Number of queries in this database that have been canceled due to deadlocks
}

func (s *Stats) fetchDatabaseConflicts() ([]DatabaseConflictsRow, error) {
	const query = `SELECT
	datid,
	datname,
	confl_tablespace,
	confl_lock,
	confl_snapshot,
	confl_bufferpin,
	confl_deadlock
	FROM pg_stat_database_conflicts`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []DatabaseConflictsRow{}
	for rows.Next() {
		var row DatabaseConflictsRow

		err := rows.Scan(
			&row.Datid,
			&row.Datname,
			&row.ConflTablespace,
			&row.ConflLock,
			&row.ConflSnapshot,
			&row.ConflBufferpin,
			&row.ConflDeadlock,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
