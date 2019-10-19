package pgstats

import "database/sql"

// IoAllTables represents content of `pg_statio_all_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-TABLES-VIEW
func (s *Stats) IoAllTables() ([]IoTablesRow, error) {
	return s.fetchIoTables("pg_statio_all_tables")
}

// IoSystemTables represents content of `pg_statio_sys_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-TABLES-VIEW
func (s *Stats) IoSystemTables() ([]IoTablesRow, error) {
	return s.fetchIoTables("pg_statio_sys_tables")
}

// IoUserTables represents content of `pg_statio_user_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-TABLES-VIEW
func (s *Stats) IoUserTables() ([]IoTablesRow, error) {
	return s.fetchIoTables("pg_statio_user_tables")
}

// IoTablesRow represents schema of pg_statio_*_tables views
type IoTablesRow struct {
	Relid         int64          `json:"relid"`           // OID of a table
	Schemaname    string         `json:"schemaname"`      // Name of the schema that this table is in
	Relname       string         `json:"relname"`         // Name of this table
	HeapBlksRead  *sql.NullInt64 `json:"heap_blks_read"`  // Number of disk blocks read from this table
	HeapBlksHit   *sql.NullInt64 `json:"heap_blks_hit"`   // Number of buffer hits in this table
	IdxBlksRead   *sql.NullInt64 `json:"idx_blks_read"`   // Number of disk blocks read from all indexes on this table
	IdxBlksHit    *sql.NullInt64 `json:"idx_blks_hit"`    // Number of buffer hits in all indexes on this table
	ToastBlksRead *sql.NullInt64 `json:"toast_blks_read"` // Number of disk blocks read from this table's TOAST table (if any)
	ToastBlksHit  *sql.NullInt64 `json:"toast_blks_hit"`  // Number of buffer hits in this table's TOAST table (if any)
	TidxBlksRead  *sql.NullInt64 `json:"tidx_blks_read"`  // Number of disk blocks read from this table's TOAST table indexes (if any)
	TidxBlksHit   *sql.NullInt64 `json:"tidx_blks_hit"`   // Number of buffer hits in this table's TOAST table indexes (if any)
}

func (s *Stats) fetchIoTables(view string) ([]IoTablesRow, error) {
	const query = `SELECT
	relid,
	schemaname,
	relname,
	heap_blks_read,
	heap_blks_hit,
	idx_blks_read,
	idx_blks_hit,
	toast_blks_read,
	toast_blks_hit,
	tidx_blks_read,
	tidx_blks_hit
	FROM `

	rows, err := s.db.Query(query + view)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []IoTablesRow{}
	for rows.Next() {
		var row IoTablesRow

		err := rows.Scan(
			&row.Relid,
			&row.Schemaname,
			&row.Relname,
			&row.HeapBlksRead,
			&row.HeapBlksHit,
			&row.IdxBlksRead,
			&row.IdxBlksHit,
			&row.ToastBlksRead,
			&row.ToastBlksHit,
			&row.TidxBlksRead,
			&row.TidxBlksHit,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
