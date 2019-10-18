package pgstats

import "database/sql"

// Activity returns rows from a `pg_stat_activity` view.
// The pg_stat_activity module provides a means for information related to the current activity of that process, such as state and current query.
//
// SeeL https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW
func (s *Stats) Activity() ([]ActivityRow, error) {
	return s.fetchActivity()
}

// ActivityRow represents schema of pg_stat_activity view
type ActivityRow struct {
	Datid           *sql.NullInt64  `json:"datid"`            // OID of the database this backend is connected to
	Datname         *sql.NullString `json:"datname"`          // Name of the database this backend is connected to
	Pid             int64           `json:"pid"`              // Process ID of this backend
	Usesysid        *sql.NullInt64  `json:"usesysid"`         // OID of the user logged into this backend
	Usename         *sql.NullString `json:"usename"`          // Name of the user logged into this backend
	ApplicationName *sql.NullString `json:"application_name"` // Name of the application that is connected to this backend
	ClientAddr      *sql.NullString `json:"client_addr"`      // IP address of the client connected to this backend.
	ClientHostname  *sql.NullString `json:"client_hostname"`  // Host name of the connected client.
	ClientPort      *sql.NullInt64  `json:"client_port"`      // TCP port number that the client is using for communication with this backend, or -1 if a Unix socket is used
	BackendStart    *sql.NullTime   `json:"backend_start"`    // Time when this process was started. For client backends, this is the time the client connected to the server.
	XactStart       *sql.NullTime   `json:"xact_start"`       // Time when this process' current transaction was started, or null if no transaction is active.
	QueryStart      *sql.NullTime   `json:"query_start"`      // Time when the currently active query was started, or if state is not active, when the last query was started
	StateChange     *sql.NullTime   `json:"state_change"`     // ime when the state was last changed
	WaitEventType   *sql.NullString `json:"wait_event_type"`  // The type of event for which the backend is waiting, if any; otherwise NULL. Supported since PostgreSQL 9.6.
	WaitEvent       *sql.NullString `json:"wait_event"`       // Wait event name if backend is currently waiting, otherwise NULL. Supported since PostgreSQL 9.6.
	Waiting         *sql.NullBool   `json:"waiting"`          // True if this backend is currently waiting on a lock. Supported until PostgreSQL 9.5 (inclusive).
	State           *sql.NullString `json:"state"`            // Current overall state of this backend. See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW
	BackendXid      *sql.NullInt64  `json:"backend_xid"`      // Top-level transaction identifier of this backend, if any.
	BackendXmin     *sql.NullInt64  `json:"backend_xmin"`     // The current backend's xmin horizon.
	Query           *sql.NullString `json:"query"`            // Text of this backend's most recent query.
	BackendType     *sql.NullString `json:"backend_type"`     // Type of current backend.
}

func (s *Stats) fetchActivity() ([]ActivityRow, error) {
	version, err := s.getVersion()
	switch {
	case err != nil:
		return nil, err
	case version > 9.6:
		return s.fetchActivity10()
	case version == 9.6:
		return s.fetchActivity96()
	default:
		return s.fetchActivity95()
	}
}

func (s *Stats) fetchActivity10() ([]ActivityRow, error) {
	const query = `SELECT
	datid,
	datname,
	pid,
	usesysid,
	usename,
	application_name,
	client_addr,
	client_hostname,
	client_port,
	backend_start,
	xact_start,
	query_start,
	state_change,
	wait_event_type,
	wait_event,
	state,
	backend_xid,
	backend_xmin,
	query,
	backend_type
	FROM pg_stat_activity`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []ActivityRow{}
	for rows.Next() {
		var row ActivityRow

		err := rows.Scan(
			&row.Datid,
			&row.Datname,
			&row.Pid,
			&row.Usesysid,
			&row.Usename,
			&row.ApplicationName,
			&row.ClientAddr,
			&row.ClientHostname,
			&row.ClientPort,
			&row.BackendStart,
			&row.XactStart,
			&row.QueryStart,
			&row.StateChange,
			&row.WaitEventType,
			&row.WaitEvent,
			&row.State,
			&row.BackendXid,
			&row.BackendXmin,
			&row.Query,
			&row.BackendType,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}

func (s *Stats) fetchActivity96() ([]ActivityRow, error) {
	const query = `SELECT
	datid,
	datname,
	pid,
	usesysid,
	usename,
	application_name,
	client_addr,
	client_hostname,
	client_port,
	backend_start,
	xact_start,
	query_start,
	state_change,
	wait_event_type,
	wait_event,
	state,
	backend_xid,
	backend_xmin,
	query
	FROM pg_stat_activity`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []ActivityRow{}
	for rows.Next() {
		var row ActivityRow

		err := rows.Scan(
			&row.Datid,
			&row.Datname,
			&row.Pid,
			&row.Usesysid,
			&row.Usename,
			&row.ApplicationName,
			&row.ClientAddr,
			&row.ClientHostname,
			&row.ClientPort,
			&row.BackendStart,
			&row.XactStart,
			&row.QueryStart,
			&row.StateChange,
			&row.WaitEventType,
			&row.WaitEvent,
			&row.State,
			&row.BackendXid,
			&row.BackendXmin,
			&row.Query,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}

func (s *Stats) fetchActivity95() ([]ActivityRow, error) {
	const query = `SELECT
	datid,
	datname,
	pid,
	usesysid,
	usename,
	application_name,
	client_addr,
	client_hostname,
	client_port,
	backend_start,
	xact_start,
	query_start,
	state_change,
	waiting,
	state,
	backend_xid,
	backend_xmin,
	query
	FROM pg_stat_activity`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []ActivityRow{}
	for rows.Next() {
		var row ActivityRow

		err := rows.Scan(
			&row.Datid,
			&row.Datname,
			&row.Pid,
			&row.Usesysid,
			&row.Usename,
			&row.ApplicationName,
			&row.ClientAddr,
			&row.ClientHostname,
			&row.ClientPort,
			&row.BackendStart,
			&row.XactStart,
			&row.QueryStart,
			&row.StateChange,
			&row.Waiting,
			&row.State,
			&row.BackendXid,
			&row.BackendXmin,
			&row.Query,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
