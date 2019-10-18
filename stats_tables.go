package pgstats

import "database/sql"

// AllTables represents content of `pg_stat_all_tables` view.
// AllTables returns a slice containing statistics about accesses
// to each table in the current database (including TOAST tables).
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW
func (s *Stats) AllTables() ([]TablesRow, error) {
	return s.fetchTable("pg_stat_all_tables")
}

// SystemTables represents content of `pg_stat_sys_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW
func (s *Stats) SystemTables() ([]TablesRow, error) {
	return s.fetchTable("pg_stat_sys_tables")
}

// UserTables represents content of `pg_stat_user_tables` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ALL-TABLES-VIEW
func (s *Stats) UserTables() ([]TablesRow, error) {
	return s.fetchTable("pg_stat_user_tables")
}

// TablesRow represents schema of pg_stat_*_tables views
type TablesRow struct {
	Relid            int64          `json:"relid"`               // OID of a table
	Schemaname       string         `json:"schemaname"`          // Name of the schema that this table is in
	Relname          string         `json:"relname"`             // Name of this table
	SeqScan          *sql.NullInt64 `json:"seq_scan"`            // Number of sequential scans initiated on this table
	SeqTupRead       *sql.NullInt64 `json:"seq_tup_read"`        // Number of live rows fetched by sequential scans
	IdxScan          *sql.NullInt64 `json:"idx_scan"`            // Number of index scans initiated on this table
	IdxTupFetch      *sql.NullInt64 `json:"idx_tup_fetch"`       // Number of live rows fetched by index scans
	NTupIns          *sql.NullInt64 `json:"n_tup_ins"`           // Number of rows inserted
	NTupUpd          *sql.NullInt64 `json:"n_tup_upd"`           // Number of rows updated (includes HOT updated rows)
	NTupDel          *sql.NullInt64 `json:"n_tup_del"`           // Number of rows deleted
	NTupHotUpd       *sql.NullInt64 `json:"n_tup_hot_upd"`       // Number of rows HOT updated (i.e., with no separate index update required)
	NLiveTup         *sql.NullInt64 `json:"n_live_tup"`          // Estimated number of live rows
	NDeadTup         *sql.NullInt64 `json:"n_dead_tup"`          // Estimated number of dead rows
	NModSinceAnalyze *sql.NullInt64 `json:"n_mod_since_analyze"` // Estimated number of rows modified since this table was last analyzed
	LastVacuum       *sql.NullTime  `json:"last_vacuum"`         // Last time at which this table was manually vacuumed (not counting VACUUM FULL)
	LastAutovacuum   *sql.NullTime  `json:"last_autovacuum"`     // Last time at which this table was vacuumed by the autovacuum daemon
	LastAnalyze      *sql.NullTime  `json:"last_analyze"`        // Last time at which this table was manually analyzed
	LastAutoanalyze  *sql.NullTime  `json:"last_autoanalyze"`    // Last time at which this table was analyzed by the autovacuum daemon
	VacuumCount      *sql.NullInt64 `json:"vacuum_count"`        // Number of times this table has been manually vacuumed (not counting VACUUM FULL)
	AutovacuumCount  *sql.NullInt64 `json:"autovacuum_count"`    // Number of times this table has been vacuumed by the autovacuum daemon
	AnalyzeCount     *sql.NullInt64 `json:"analyze_count"`       // Number of times this table has been manually analyzed
	AutoanalyzeCount *sql.NullInt64 `json:"autoanalyze_count"`   // Number of times this table has been analyzed by the autovacuum daemon
}

func (s *Stats) fetchTable(view string) ([]TablesRow, error) {
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
	n_tup_hot_upd,
	n_live_tup,
	n_dead_tup,
	n_mod_since_analyze,
	last_vacuum,
	last_autovacuum,
	last_analyze,
	last_autoanalyze,
	vacuum_count,
	autovacuum_count,
	analyze_count,
	autoanalyze_count
	FROM `

	rows, err := s.db.Query(query + view)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []TablesRow{}
	for rows.Next() {
		var row TablesRow

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
			&row.NLiveTup,
			&row.NDeadTup,
			&row.NModSinceAnalyze,
			&row.LastVacuum,
			&row.LastAutovacuum,
			&row.LastAnalyze,
			&row.LastAutoanalyze,
			&row.VacuumCount,
			&row.AutovacuumCount,
			&row.AnalyzeCount,
			&row.AutoanalyzeCount,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
