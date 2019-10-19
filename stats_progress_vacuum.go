package pgstats

import (
	"database/sql"
	"fmt"
)

// ProgressVacuum represents content of `pg_stat_progress_vacuum` view.
// One row for each backend (including autovacuum worker processes) that is currently vacuuming.
//
// See: https://www.postgresql.org/docs/current/progress-reporting.html#VACUUM-PROGRESS-REPORTING
func (s *Stats) ProgressVacuum() ([]ProgressVacuumRow, error) {
	return s.fetchProgressVacuum()
}

// ProgressVacuumRow represents schema of pg_stat_progress_vacuum view
type ProgressVacuumRow struct {
	Pid              int64          `json:"pid"`                // Process ID of backend.
	Datid            int64          `json:"datid"`              // OID of the database to which this backend is connected.
	Datname          string         `json:"datname"`            // Name of the database to which this backend is connected.
	Relid            int64          `json:"relid"`              // OID of the table being vacuumed.
	Phase            string         `json:"phase"`              // Current processing phase of vacuum.
	HeapBlksTotal    *sql.NullInt64 `json:"heap_blks_total"`    // Total number of heap blocks in the table.
	HeapBlksScanned  *sql.NullInt64 `json:"heap_blks_scanned"`  // Number of heap blocks scanned.
	HeapBlksVacuumed *sql.NullInt64 `json:"heap_blks_vacuumed"` // Number of heap blocks vacuumed.
	IndexVacuumCount *sql.NullInt64 `json:"index_vacuum_count"` // Number of completed index vacuum cycles.
	MaxDeadTuples    *sql.NullInt64 `json:"max_dead_tuples"`    // Number of dead tuples that we can store before needing to perform an index vacuum cycle, based on maintenance_work_mem.
	NumDeadTuples    *sql.NullInt64 `json:"num_dead_tuples"`    // Number of dead tuples collected since the last index vacuum cycle.
}

func (s *Stats) fetchProgressVacuum() ([]ProgressVacuumRow, error) {
	version, err := s.getVersion()
	switch {
	case err != nil:
		return nil, err
	case version < 9.6:
		return nil, fmt.Errorf("Unsupported PostgreSQL version: %f", version)
	default:
		// pass
	}

	const query = `SELECT
	pid,
	datid,
	datname,
	relid,
	phase,
	heap_blks_total,
	heap_blks_scanned,
	heap_blks_vacuumed,
	index_vacuum_count,
	max_dead_tuples,
	num_dead_tuples
	FROM pg_stat_progress_vacuum`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []ProgressVacuumRow{}
	for rows.Next() {
		var row ProgressVacuumRow

		err := rows.Scan(
			&row.Pid,
			&row.Datid,
			&row.Datname,
			&row.Relid,
			&row.Phase,
			&row.HeapBlksTotal,
			&row.HeapBlksScanned,
			&row.HeapBlksVacuumed,
			&row.IndexVacuumCount,
			&row.MaxDeadTuples,
			&row.NumDeadTuples,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
