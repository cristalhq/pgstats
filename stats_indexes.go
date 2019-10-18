package pgstats

import "database/sql"

// AllIndexes represents content of `pg_stat_all_indexes` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
func (s *Stats) AllIndexes() ([]IndexesRow, error) {
	return s.fetchIndexes("pg_stat_all_indexes")
}

// SystemIndexes represents content of `pg_stat_system_indexes` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
func (s *Stats) SystemIndexes() ([]IndexesRow, error) {
	return s.fetchIndexes("pg_stat_sys_indexes")
}

// UserIndexes represents content of `pg_stat_user_indexes` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-INDEXES-VIEW
func (s *Stats) UserIndexes() ([]IndexesRow, error) {
	return s.fetchIndexes("pg_stat_user_indexes")
}

// IndexesRow represents schema of pg_stat_*_indexes views.
type IndexesRow struct {
	Relid        int64          `json:"relid"`         // OID of the table for this index
	Indexrelid   int64          `json:"indexrelid"`    // OID of this index
	Schemaname   string         `json:"schemaname"`    // Name of the schema this index is in
	Relname      string         `json:"relname"`       // Name of the table for this index
	Indexrelname string         `json:"indexrelname"`  // Name of this index
	IdxScan      *sql.NullInt64 `json:"idx_scan"`      // Number of index scans initiated on this index
	IdxTupRead   *sql.NullInt64 `json:"idx_tup_read"`  // Number of index entries returned by scans on this index
	IdxTupFetch  *sql.NullInt64 `json:"idx_tup_fetch"` // Number of live table rows fetched by simple index scans using this index
}

func (s *Stats) fetchIndexes(view string) ([]IndexesRow, error) {
	const query = `SELECT
	relid
	indexrelid
	schemaname
	relname
	indexrelname
	idx_scan
	idx_tup_read
	idx_tup_fetch from `

	rows, err := s.db.Query(query + view)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []IndexesRow{}
	for rows.Next() {
		var row IndexesRow

		err := rows.Scan(
			&row.Relid,
			&row.Indexrelid,
			&row.Schemaname,
			&row.Relname,
			&row.Indexrelname,
			&row.IdxScan,
			&row.IdxTupRead,
			&row.IdxTupFetch,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
