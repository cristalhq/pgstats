package pgstats

import "database/sql"

// Database returns rows from a `pg_stat_database` view.
// One row per database, showing database-wide statistics.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-DATABASE-VIEW
func (s *Stats) Database() ([]DatabaseRow, error) {
	return s.fetchDatabases()
}

// DatabaseRow represents schema of pg_stat_database view
type DatabaseRow struct {
	Datid        int64            `json:"datid"`          // OID of a database
	Datname      string           `json:"datname"`        // Name of this database
	NumBackends  int64            `json:"numbackends"`    // Number of backends currently connected to this database.
	XactCommit   *sql.NullInt64   `json:"xact_commit"`    // Number of transactions in this database that have been committed
	XactRollback *sql.NullInt64   `json:"xact_rollback"`  //	Number of transactions in this database that have been rolled back
	BlksRead     *sql.NullInt64   `json:"blks_read"`      // Number of disk blocks read in this database
	BlksHit      *sql.NullInt64   `json:"blks_hit"`       // Number of times disk blocks were found already in the buffer cache, so that a read was not necessary
	TupReturned  *sql.NullInt64   `json:"tup_returned"`   // Number of rows returned by queries in this database
	TupFetched   *sql.NullInt64   `json:"tup_fetched"`    // Number of rows fetched by queries in this database
	TupInserted  *sql.NullInt64   `json:"tup_inserted"`   // Number of rows inserted by queries in this database
	TupUpdated   *sql.NullInt64   `json:"tup_updated"`    // Number of rows updated by queries in this database
	TupDeleted   *sql.NullInt64   `json:"tup_deleted"`    // 	Number of rows deleted by queries in this database
	Conflicts    *sql.NullInt64   `json:"conflicts"`      // Number of queries canceled due to conflicts with recovery in this database.
	TempFiles    *sql.NullInt64   `json:"temp_files"`     // Number of temporary files created by queries in this database.
	TempBytes    *sql.NullInt64   `json:"temp_bytes"`     // Total amount of data written to temporary files by queries in this database.
	Deadlocks    *sql.NullInt64   `json:"deadlocks"`      // Number of deadlocks detected in this database
	BlkReadTime  *sql.NullFloat64 `json:"blk_read_time"`  // Time spent reading data file blocks by backends in this database, in milliseconds
	BlkWriteTime *sql.NullFloat64 `json:"blk_write_time"` // Time spent writing data file blocks by backends in this database, in milliseconds
	StatsReset   *sql.NullTime    `json:"stats_reset"`    // Time at which these statistics were last reset
}

func (s *Stats) fetchDatabases() ([]DatabaseRow, error) {
	const query = `SELECT
	datid,
	datname,
	numbackends,
	xact_commit,
	xact_rollback,
	blks_read,
	blks_hit,
	tup_returned,
	tup_fetched,
	tup_inserted,
	tup_updated,
	tup_deleted,
	conflicts,
	temp_files,
	temp_bytes,
	deadlocks,
	blk_read_time,
	blk_write_time,
	stats_reset
	FROM pg_stat_database`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []DatabaseRow{}
	for rows.Next() {
		var row DatabaseRow

		err := rows.Scan(
			&row.Datid,
			&row.Datname,
			&row.NumBackends,
			&row.XactCommit,
			&row.XactRollback,
			&row.BlksRead,
			&row.BlksHit,
			&row.TupReturned,
			&row.TupFetched,
			&row.TupInserted,
			&row.TupUpdated,
			&row.TupDeleted,
			&row.Conflicts,
			&row.TempFiles,
			&row.TempBytes,
			&row.Deadlocks,
			&row.BlkReadTime,
			&row.BlkWriteTime,
			&row.StatsReset,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
