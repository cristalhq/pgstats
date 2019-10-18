package pgstats

import "database/sql"

// IoAllIndexes represents content of `pg_statio_all_indexes` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
func (s *Stats) IoAllIndexes() ([]IoIndexesRow, error) {
	return s.fetchIoIndexes("pg_statio_all_indexes")
}

// IoSystemIndexesView represents content of `pg_statio_system_indexes` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
func (s *Stats) IoSystemIndexes() ([]IoIndexesRow, error) {
	return s.fetchIoIndexes("pg_statio_sys_indexes")
}

// IoUserIndexes represents content of `pg_statio_user_indexes` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-INDEXES-VIEW
func (s *Stats) IoUserIndexes() ([]IoIndexesRow, error) {
	return s.fetchIoIndexes("pg_statio_user_indexes")
}

// IoIndexesRow represents schema of `pg_statio_*_indexes` views.
type IoIndexesRow struct {
	Relid        int64          `json:"relid"`         // OID of the table for this index
	Indexrelid   int64          `json:"indexrelid"`    // OID of this index
	Schemaname   string         `json:"schemaname"`    // Name of the schema this index is in
	Relname      string         `json:"relname"`       // Name of the table for this index
	Indexrelname string         `json:"indexrelname"`  // Name of this index
	IdxBlksRead  *sql.NullInt64 `json:"idx_blks_read"` // Number of disk blocks read from this index
	IdxBlksHit   *sql.NullInt64 `json:"idx_blks_hit"`  // Number of buffer hits in this index
}

func (s *Stats) fetchIoIndexes(table string) ([]IoIndexesRow, error) {
	const query = `SELECT
	relid,
	indexrelid,
	schemaname,
	relname,
	indexrelname,
	idx_blks_read,
	idx_blks_hit
	FROM `

	rows, err := s.db.Query(query + table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []IoIndexesRow{}
	for rows.Next() {
		var row IoIndexesRow

		err := rows.Scan(
			&row.Relid,
			&row.Indexrelid,
			&row.Schemaname,
			&row.Relname,
			&row.Indexrelname,
			&row.IdxBlksRead,
			&row.IdxBlksHit,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, nil
}
