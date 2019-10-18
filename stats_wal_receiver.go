package pgstats

import (
	"database/sql"
	"fmt"
)

// WalReceiver returns rows from a `pg_stat_wal_receiver` view.
// Only one row, showing statistics about the WAL receiver from that receiver's connected server.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-WAL-RECEIVER-VIEW
func (s *Stats) WalReceiver() (WalReceiverView, error) {
	return s.fetchWalReceiver()
}

// WalReceiverView represents content of pg_stat_wal_receiver view
type WalReceiverView struct {
	Pid                int64           `json:"pid"`                   // Process ID of the WAL receiver process
	Status             string          `json:"status"`                // Activity status of the WAL receiver process
	ReceiveStartLsn    *sql.NullInt64  `json:"receive_start_lsn"`     // First write-ahead log location used when WAL receiver is started
	ReceiveStartTli    *sql.NullInt64  `json:"receive_start_tli"`     // First timeline number used when WAL receiver is started
	ReceivedLsn        *sql.NullInt64  `json:"received_lsn"`          // Last write-ahead log location already received and flushed to disk,
	ReceivedTli        *sql.NullInt64  `json:"received_tli"`          // Timeline number of last write-ahead log location received and flushed to disk.
	LastMsgSendTime    *sql.NullTime   `json:"last_msg_send_time"`    // Send time of last message received from origin WAL sender
	LastMsgReceiptTime *sql.NullTime   `json:"last_msg_receipt_time"` // Receipt time of last message received from origin WAL sender
	LatestEndLsn       *sql.NullInt64  `json:"latest_end_lsn"`        // Last write-ahead log location reported to origin WAL sender
	LatestEndTime      *sql.NullTime   `json:"latest_end_time"`       // Time of last write-ahead log location reported to origin WAL sender
	SlotName           *sql.NullString `json:"slot_name"`             // Replication slot name used by this WAL receiver
	SenderHost         *sql.NullString `json:"sender_host"`           // Host of the PostgreSQL instance this WAL receiver is connected to.
	SenderPort         *sql.NullInt64  `json:"sender_port"`           // Port number of the PostgreSQL instance this WAL receiver is connected to. Supported since PostgreSQL 11.
	Conninfo           *sql.NullString `json:"conninfo"`              // Connection string used by this WAL receiver, with security-sensitive fields obfuscated.
}

func (s *Stats) fetchWalReceiver() (WalReceiverView, error) {
	version, err := s.getVersion()
	switch {
	case err != nil:
		return WalReceiverView{}, err
	case version > 10:
		return s.fetchWalReceiver11()
	case version == 10 || version == 9.6:
		return s.fetchWalReceiver10()
	default:
		return WalReceiverView{}, fmt.Errorf("Unsupported PostgreSQL version: %f", version)
	}
}

func (s *Stats) fetchWalReceiver11() (WalReceiverView, error) {
	const query = `SELECT
	pid,
	status,
	receive_start_lsn,
	receive_start_tli,
	received_lsn,
	received_tli,
	last_msg_send_time,
	last_msg_receipt_time,
	latest_end_lsn,
	latest_end_time,
	slot_name,
	sender_host,
	sender_port,
	conninfo
	FROM pg_stat_wal_receiver`

	row := s.db.QueryRow(query)
	var res WalReceiverView

	err := row.Scan(
		&res.Pid,
		&res.Status,
		&res.ReceiveStartLsn,
		&res.ReceiveStartTli,
		&res.ReceivedLsn,
		&res.ReceivedTli,
		&res.LastMsgSendTime,
		&res.LastMsgReceiptTime,
		&res.LatestEndLsn,
		&res.LatestEndTime,
		&res.SlotName,
		&res.Conninfo,
	)
	return res, err
}

func (s *Stats) fetchWalReceiver10() (WalReceiverView, error) {
	const query = `SELECT
	pid,
	status,
	receive_start_lsn,
	receive_start_tli,
	received_lsn,
	received_tli,
	last_msg_send_time,
	last_msg_receipt_time,
	latest_end_lsn,
	latest_end_time,
	slot_name,
	conninfo
	FROM pg_stat_wal_receiver`

	row := s.db.QueryRow(query)
	var res WalReceiverView

	err := row.Scan(
		&res.Pid,
		&res.Status,
		&res.ReceiveStartLsn,
		&res.ReceiveStartTli,
		&res.ReceivedLsn,
		&res.ReceivedTli,
		&res.LastMsgSendTime,
		&res.LastMsgReceiptTime,
		&res.LatestEndLsn,
		&res.LatestEndTime,
		&res.SlotName,
		&res.Conninfo,
	)
	return res, err
}
