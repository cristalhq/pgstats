package pgstats

import "database/sql"

// UserFunctions represents content of `pg_stat_user_functions` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-USER-FUNCTIONS-VIEW
func (s *Stats) UserFunctions() ([]FunctionsRow, error) {
	return s.fetchFunctions("pg_stat_user_functions")
}

// XactUserFunctionsView represents content of `pg_stat_xact_user_functions` view.
// Similar to pg_stat_user_functions, but counts only calls during the current transaction (which are not yet included in pg_stat_user_functions).
//
func (s *Stats) XactUserFunctions() ([]FunctionsRow, error) {
	return s.fetchFunctions("pg_stat_xact_user_functions")
}

// FunctionsRow represents schema of pg_stat*_user_functions views
type FunctionsRow struct {
	Funcid     int64            `json:"funcid"`     // OID of a function
	Schemaname string           `json:"schemaname"` // Name of the schema this function is in
	Funcname   string           `json:"funcname"`   // Name of this function
	Calls      *sql.NullInt64   `json:"calls"`      // Number of times this function has been called
	TotalTime  *sql.NullFloat64 `json:"total_time"` // Total time spent in this function and all other functions called by it, in milliseconds
	SelfTime   *sql.NullFloat64 `json:"self_time"`  // Total time spent in this function itself, not including other functions called by it, in milliseconds
}

func (s *Stats) fetchFunctions(view string) ([]FunctionsRow, error) {
	const query = `SELECT funcid, schemaname, funcname, calls, total_time, self_time FROM `

	rows, err := s.db.Query(query + view)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []FunctionsRow{}
	for rows.Next() {
		var row FunctionsRow

		err := rows.Scan(
			&row.Funcid,
			&row.Schemaname,
			&row.Funcname,
			&row.Calls,
			&row.TotalTime,
			&row.SelfTime,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
