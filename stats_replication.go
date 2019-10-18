package pgstats

import (
	"database/sql"
)

// Replication represents content of pg_stat_replication view
// One row per WAL sender process, showing statistics about replication to that sender's connected standby server.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-REPLICATION-VIEW
func (s *Stats) Replication() ([]ReplicationRow, error) {
	return s.fetchReplication()
}

// ReplicationRow represents schema of pg_stat_replication view
type ReplicationRow struct {
	Pid             int64           `json:"pid"`              // Process ID of a WAL sender process
	Usesysid        *sql.NullInt64  `json:"usesysid"`         // OID of the user logged into this WAL sender process
	Usename         *sql.NullString `json:"usename"`          // Name of the user logged into this WAL sender process
	ApplicationName *sql.NullString `json:"application_name"` // Name of the application that is connected to this WAL sender
	ClientAddr      *sql.NullString `json:"client_addr"`      // IP address of the client connected to this WAL sender.
	ClientHostname  *sql.NullString `json:"client_hostname"`  // Host name of the connected client, as reported by a reverse DNS lookup of client_addr.
	ClientPort      *sql.NullInt64  `json:"client_port"`      // TCP port number that the client is using for communication with this WAL sender, or -1 if a Unix socket is used
	BackendStart    *sql.NullTime   `json:"backend_start"`    // Time when this process was started, i.e., when the client connected to this WAL sender
	BackendXmin     *sql.NullInt64  `json:"backend_xmin"`     // This standby's xmin horizon reported by hot_standby_feedback - see:
	State           *sql.NullString `json:"state"`            // Current WAL sender state.
	SentLsn         *sql.NullInt64  `json:"sent_lsn"`         // Last write-ahead log location sent on this connection
	WriteLsn        *sql.NullInt64  `json:"write_lsn"`        // Last write-ahead log location written to disk by this standby server
	FlushLsn        *sql.NullInt64  `json:"flush_lsn"`        // Last write-ahead log location flushed to disk by this standby server
	ReplayLsn       *sql.NullInt64  `json:"replay_lsn"`       // Last write-ahead log location replayed into the database on this standby server
	WriteLag        *sql.NullTime   `json:"write_lag"`        // Time elapsed between flushing recent WAL locally and receiving notification that this standby server
	FlushLag        *sql.NullTime   `json:"flush_lag"`        // Time elapsed between flushing recent WAL locally and receiving notification that this standby server.
	ReplayLag       *sql.NullTime   `json:"replay_lag"`       // Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written, flushed and applied it.
	SyncPriority    *sql.NullInt64  `json:"sync_priority"`    // Priority of this standby server for being chosen as the synchronous standby in a priority-based synchronous replication.
	SyncState       *sql.NullString `json:"sync_state"`       // Synchronous state of this standby server.
}

func (s *Stats) fetchReplication() ([]ReplicationRow, error) {
	version, err := s.getVersion()
	switch {
	case err != nil:
		return nil, err
	case version < 10:
		return s.fetchReplication96()
	default:
		return s.fetchReplication10()
	}
}

func (s *Stats) fetchReplication10() ([]ReplicationRow, error) {
	const query = `SELECT
	pid,
	usesysid,
	usename,
	application_name,
	client_addr,
	client_hostname,
	client_port,
	backend_start,
	backend_xmin,
	state,
	sent_lsn,
	write_lsn,
	flush_lsn,
	replay_lsn,
	write_lag,
	flush_lag,
	replay_lag,
	sync_priority,
	sync_state
	FROM pg_stat_replication`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []ReplicationRow{}
	for rows.Next() {
		var row ReplicationRow

		err := rows.Scan(
			&row.Pid,
			&row.Usesysid,
			&row.Usename,
			&row.ApplicationName,
			&row.ClientAddr,
			&row.ClientHostname,
			&row.ClientPort,
			&row.BackendStart,
			&row.BackendXmin,
			&row.State,
			&row.SentLsn,
			&row.WriteLsn,
			&row.FlushLsn,
			&row.ReplayLsn,
			&row.WriteLag,
			&row.FlushLag,
			&row.ReplayLag,
			&row.SyncPriority,
			&row.SyncState,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}

func (s *Stats) fetchReplication96() ([]ReplicationRow, error) {
	const query = `SELECT
	pid,
	usesysid,
	usename,
	application_name,
	client_addr,
	client_hostname,
	client_port,
	backend_start,
	backend_xmin,
	state,
	sent_location,
	write_location,
	flush_location,
	replay_location,
	sync_priority,
	sync_state
	FROM pg_stat_replication`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []ReplicationRow{}
	for rows.Next() {
		var row ReplicationRow

		err := rows.Scan(
			&row.Pid,
			&row.Usesysid,
			&row.Usename,
			&row.ApplicationName,
			&row.ClientAddr,
			&row.ClientHostname,
			&row.ClientPort,
			&row.BackendStart,
			&row.BackendXmin,
			&row.State,
			&row.SentLsn,
			&row.WriteLsn,
			&row.FlushLsn,
			&row.ReplayLsn,
			&row.SyncPriority,
			&row.SyncState,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
