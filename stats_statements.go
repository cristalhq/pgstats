package pgstats

// Statements returns rows from a `pg_stat_statements` view.
// The pg_stat_statements module provides a means for tracking execution statistics of all SQL statements executed by a server.
//
// See: https://www.postgresql.org/docs/current/pgstatstatements.html
func (s *Stats) Statements() ([]StatementsRow, error) {
	return s.fetchStatements()
}

// StatementsRow represents rows of pg_stat_statements view.
type StatementsRow struct {
	Userid            int64   `json:"userid"`              // OID of user who executed the statement
	Dbid              int64   `json:"dbid"`                // OID of database in which the statement was executed
	Queryid           int64   `json:"queryid"`             // Internal hash code, computed from the statement's parse tree
	Query             string  `json:"query"`               // Text of a representative statement
	Calls             int64   `json:"calls"`               // Number of times executed
	TotalTime         float64 `json:"total_time"`          // Total time spent in the statement, in milliseconds.
	MinTime           float64 `json:"min_time"`            // Minimum time spent in the statement, in milliseconds.
	MaxTime           float64 `json:"max_time"`            // Maximum time spent in the statement, in milliseconds.
	MeanTime          float64 `json:"mean_time"`           // Mean time spent in the statement, in milliseconds.
	StddevTime        float64 `json:"stddev_time"`         // Population standard deviation of time spent in the statement, in milliseconds.
	Rows              int64   `json:"rows"`                // Total number of rows retrieved or affected by the statement
	SharedBlksHit     int64   `json:"shared_blks_hit"`     // Total number of shared block cache hits by the statement
	SharedBlksRead    int64   `json:"shared_blks_read"`    // Total number of shared blocks read by the statement
	SharedBlksDirtied int64   `json:"shared_blks_dirtied"` // Total number of shared blocks dirtied by the statement
	SharedBlksWritten int64   `json:"shared_blks_written"` // Total number of shared blocks written by the statement
	LocalBlksHit      int64   `json:"local_blks_hit"`      // Total number of local block cache hits by the statement
	LocalBlksRead     int64   `json:"local_blks_read"`     // Total number of local blocks read by the statement
	LocalBlksDirtied  int64   `json:"local_blks_dirtied"`  // Total number of local blocks dirtied by the statement
	LocalBlksWritten  int64   `json:"local_blks_written"`  // Total number of local blocks written by the statement
	TempBlksRead      int64   `json:"temp_blks_read"`      // Total number of temp blocks read by the statement
	TempBlksWritten   int64   `json:"temp_blks_written"`   // Total number of temp blocks written by the statement
	BlkReadTime       float64 `json:"blk_read_time"`       // Total time the statement spent reading blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
	BlkWriteTime      float64 `json:"blk_write_time"`      // Total time the statement spent writing blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
}

func (s *Stats) fetchStatements() ([]StatementsRow, error) {
	version, err := s.getVersion()
	switch {
	case err != nil:
		return nil, err
	case version > 9.4:
		return s.fetchStatements95()
	default:
		return s.fetchStatements94()
	}
}

func (s *Stats) fetchStatements95() ([]StatementsRow, error) {
	const query = `SELECT
	userid,
	dbid,
	queryid,
	query,
	calls,
	total_time,
	min_time,
	max_time,
	mean_time,
	stddev_time,
	rows,
	shared_blks_hit,
	shared_blks_read,
	shared_blks_dirtied,
	shared_blks_written,
	local_blks_hit,
	local_blks_read,
	local_blks_dirtied,
	local_blks_written,
	temp_blks_read,
	temp_blks_written,
	blk_read_time,
	blk_write_time
	FROM pg_stat_statements`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []StatementsRow{}
	for rows.Next() {
		var row StatementsRow

		err := rows.Scan(
			&row.Userid,
			&row.Dbid,
			&row.Queryid,
			&row.Query,
			&row.Calls,
			&row.TotalTime,
			&row.MinTime,
			&row.MaxTime,
			&row.MeanTime,
			&row.StddevTime,
			&row.Rows,
			&row.SharedBlksHit,
			&row.SharedBlksRead,
			&row.SharedBlksDirtied,
			&row.SharedBlksWritten,
			&row.LocalBlksHit,
			&row.LocalBlksRead,
			&row.LocalBlksDirtied,
			&row.LocalBlksWritten,
			&row.TempBlksRead,
			&row.TempBlksWritten,
			&row.BlkReadTime,
			&row.BlkWriteTime,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}

func (s *Stats) fetchStatements94() ([]StatementsRow, error) {
	const query = `SELECT
	userid
	dbid
	queryid
	query
	calls
	rows
	shared_blks_hit
	shared_blks_read
	shared_blks_dirtied
	shared_blks_written
	local_blks_hit
	local_blks_read
	local_blks_dirtied
	local_blks_written
	temp_blks_read
	temp_blks_written
	blk_read_time
	blk_write_time
	FROM pg_stat_statements`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []StatementsRow{}
	for rows.Next() {
		var row StatementsRow

		err := rows.Scan(
			&row.Userid,
			&row.Dbid,
			&row.Queryid,
			&row.Query,
			&row.Calls,
			&row.Rows,
			&row.SharedBlksHit,
			&row.SharedBlksRead,
			&row.SharedBlksDirtied,
			&row.SharedBlksWritten,
			&row.LocalBlksHit,
			&row.LocalBlksRead,
			&row.LocalBlksDirtied,
			&row.LocalBlksWritten,
			&row.TempBlksRead,
			&row.TempBlksWritten,
			&row.BlkReadTime,
			&row.BlkWriteTime,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
