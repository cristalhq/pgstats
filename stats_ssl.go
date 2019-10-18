package pgstats

import (
	"database/sql"
	"fmt"
)

// Ssl represents content of `pg_stat_ssl` view.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-SSL
func (s *Stats) Ssl() ([]SslRow, error) {
	return s.fetchSsl()
}

// SslRow represents schema of pg_stat_ssl view.
type SslRow struct {
	Pid         int64           `json:"pid"`         // Process ID of a backend or WAL sender process
	Ssl         bool            `json:"ssl"`         // True if SSL is used on this connection
	Version     *sql.NullString `json:"version"`     // Version of SSL in use, or NULL if SSL is not in use on this connection
	Cipher      *sql.NullString `json:"cipher"`      // Name of SSL cipher in use, or NULL if SSL is not in use on this connection
	Bits        *sql.NullInt64  `json:"bits"`        // Number of bits in the encryption algorithm used, or NULL if SSL is not used on this connection
	Compression *sql.NullBool   `json:"compression"` // True if SSL compression is in use, false if not, or NULL if SSL is not in use on this connection
	Clientdn    *sql.NullString `json:"clientdn"`    // Distinguished Name (DN) field from the client certificate used.
}

func (s *Stats) fetchSsl() ([]SslRow, error) {
	version, err := s.getVersion()
	switch {
	case err != nil:
		return nil, err
	case version < 9.5:
		return nil, fmt.Errorf("Unsupported PostgreSQL version: %f", version)
	}

	const query = `SELECT
	pid,
	ssl,
	version,
	cipher,
	bits,
	compression,
	clientdn
	FROM pg_stat_ssl`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []SslRow{}
	for rows.Next() {
		var row SslRow

		err := rows.Scan(
			&row.Pid,
			&row.Ssl,
			&row.Version,
			&row.Cipher,
			&row.Bits,
			&row.Compression,
			&row.Clientdn,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
