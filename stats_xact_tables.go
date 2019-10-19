package pgstats

import "database/sql"

// XactAllTables represents content of `pg_stat_xact_all_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS-VIEWS
func (s *Stats) XactAllTables() ([]XactTablesRow, error) {
	return s.fetchXactTables("pg_stat_xact_all_tables")
}

// XactSystemTables represents content of `pg_stat_xact_sys_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS-VIEWS
func (s *Stats) XactSystemTables() ([]XactTablesRow, error) {
	return s.fetchXactTables("pg_stat_xact_sys_tables")
}

// XactUserTables represents content of `pg_stat_xact_user_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-STATS-VIEWS
func (s *Stats) XactUserTables() ([]XactTablesRow, error) {
	return s.fetchXactTables("pg_stat_xact_user_tables")
}

// XactTablesRow represents schema of pg_stat_xact_*_tables views
type XactTablesRow struct {
	Relid       int64          `json:"relid"`         // OID of a table
	Schemaname  string         `json:"schemaname"`    // Name of the schema that this table is in
	Relname     string         `json:"relname"`       // Name of this table
	SeqScan     *sql.NullInt64 `json:"seq_scan"`      // Number of sequential scans initiated on this table
	SeqTupRead  *sql.NullInt64 `json:"seq_tup_read"`  // Number of live rows fetched by sequential scans
	IdxScan     *sql.NullInt64 `json:"idx_scan"`      // Number of index scans initiated on this table
	IdxTupFetch *sql.NullInt64 `json:"idx_tup_fetch"` // Number of live rows fetched by index scans
	NTupIns     *sql.NullInt64 `json:"n_tup_ins"`     // Number of rows inserted
	NTupUpd     *sql.NullInt64 `json:"n_tup_upd"`     // Number of rows updated (includes HOT updated rows)
	NTupDel     *sql.NullInt64 `json:"n_tup_del"`     // Number of rows deleted
	NTupHotUpd  *sql.NullInt64 `json:"n_tup_hot_upd"` // Number of rows HOT updated (i.e., with no separate index update required)
}

func (s *Stats) fetchXactTables(view string) ([]XactTablesRow, error) {
	const query = `SELECT
	relid,
	schemaname,
	relname,
	seq_scan,
	seq_tup_read,
	idx_scan,
	idx_tup_fetch,
	n_tup_ins,
	n_tup_upd,
	n_tup_del,
	n_tup_hot_upd
	FROM `

	rows, err := s.db.Query(query + view)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []XactTablesRow{}
	for rows.Next() {
		var row XactTablesRow

		err := rows.Scan(
			&row.Relid,
			&row.Schemaname,
			&row.Relname,
			&row.SeqScan,
			&row.SeqTupRead,
			&row.IdxScan,
			&row.IdxTupFetch,
			&row.NTupIns,
			&row.NTupUpd,
			&row.NTupDel,
			&row.NTupHotUpd,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
