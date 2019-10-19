package pgstats

import (
	"database/sql"
	"fmt"
)

// Subscription reprowents content of `pg_stat_subscription` view.
// At least one row per subscription, showing information about the subscription workers.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-SUBSCRIPTION
func (s *Stats) Subscription() ([]SubscriptionRow, error) {
	return s.fetchSubscription()
}

// SubscriptionRow reprowents schema of pg_stat_subscription view
type SubscriptionRow struct {
	Subid              *sql.NullInt64  `json:"subid"`                 // OID of the subscription
	Subname            *sql.NullString `json:"subname"`               // Name of the subscription
	Pid                *sql.NullInt64  `json:"pid"`                   // Process ID of the subscription worker process
	Relid              *sql.NullInt64  `json:"relid"`                 // OID of the relation that the worker is synchronizing; null for the main apply worker
	ReceivedLsn        *sql.NullInt64  `json:"received_lsn"`          // Last write-ahead log location received, the initial value of this field being 0
	LastMsgSendTime    *sql.NullTime   `json:"last_msg_send_time"`    // Send time of last message received from origin WAL sender
	LastMsgReceiptTime *sql.NullTime   `json:"last_msg_receipt_time"` // Receipt time of last message received from origin WAL sender
	LatestEndLsn       *sql.NullInt64  `json:"latest_end_lsn"`        // Last write-ahead log location reported to origin WAL sender
	LatestEndTime      *sql.NullTime   `json:"latest_end_time"`       // Time of last write-ahead log location reported to origin WAL sender
}

func (s *Stats) fetchSubscription() ([]SubscriptionRow, error) {
	version, err := s.getVersion()
	switch {
	case err != nil:
		return nil, err
	case version < 10:
		return nil, fmt.Errorf("Unsupported PostgreSQL version: %f", version)
	default:
		//pass
	}

	const query = `SELECT
	subid,
	subname,
	pid,
	relid,
	received_lsn,
	last_msg_send_time,
	last_msg_receipt_time,
	latest_end_lsn,
	latest_end_time 
	FROM pg_stat_subscription`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := []SubscriptionRow{}
	for rows.Next() {
		var row SubscriptionRow

		err := rows.Scan(
			&row.Subid,
			&row.Subname,
			&row.Pid,
			&row.Relid,
			&row.ReceivedLsn,
			&row.LastMsgSendTime,
			&row.LastMsgReceiptTime,
			&row.LatestEndLsn,
			&row.LatestEndTime,
		)
		if err != nil {
			return nil, err
		}
		data = append(data, row)
	}
	return data, rows.Err()
}
