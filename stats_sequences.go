package pgstats

import "database/sql"

// IoAllSequences represents content of `pg_statio_all_sequences` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-SEQUENCES-VIEW
func (s *Stats) IoAllSequences() ([]IoSequencesRow, error) {
	return s.fetchIoSequences("pg_statio_all_sequences")
}

// IoSystemSequences represents content of `pg_statio_sys_sequences` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-SEQUENCES-VIEW
func (s *Stats) IoSystemSequences() ([]IoSequencesRow, error) {
	return s.fetchIoSequences("pg_statio_sys_sequences")
}

// IoUserSequences represents content of `pg_statio_user_sequences` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STATIO-ALL-SEQUENCES-VIEW
func (s *Stats) IoUserSequences() ([]IoSequencesRow, error) {
	return s.fetchIoSequences("pg_statio_user_sequences")
}

// IoSequencesRow represents schema of pg_statio_*_sequences views
type IoSequencesRow struct {
	Relid      int64          `json:"relid"`      // OID of a sequence
	Schemaname string         `json:"schemaname"` // Name of the schema this sequence is in
	Relname    string         `json:"relname"`    // Name of this sequence
	BlksRead   *sql.NullInt64 `json:"blks_read"`  // Number of disk blocks read from this sequence
	BlksHit    *sql.NullInt64 `json:"blks_hit"`   // Number of buffer hits in this sequence
}

func (s *Stats) fetchIoSequences(view string) ([]IoSequencesRow, error) {
	const query = `SELECT
	relid,
	schemaname,
	relname,
	blks_read,
	blks_hit
	FROM `

	rows, err := s.db.Query(query + view)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []IoSequencesRow{}
	for rows.Next() {
		var row IoSequencesRow

		err := rows.Scan(
			&row.Relid,
			&row.Schemaname,
			&row.Relname,
			&row.BlksRead,
			&row.BlksHit,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
