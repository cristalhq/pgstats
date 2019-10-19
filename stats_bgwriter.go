package pgstats

import "database/sql"

// BgWriter returns rows from a `pg_stat_bgwriter` view.
// One row only, showing statistics about the background writer process's activity. See pg_stat_bgwriter for details.
//
// See: https://www.postgresql.org/docs/current/monitoring-stats.html#PG-STAT-BGWRITER-VIEW
func (s *Stats) BgWriter() (BgWriterView, error) {
	return s.fetchBgWriter()
}

// BgWriterView represents content of pg_stat_bgwriter view
type BgWriterView struct {
	CheckpointsTimed    *sql.NullInt64   `json:"checkpoints_timed"`     // Number of scheduled checkpoints that have been performed
	CheckpointsReq      *sql.NullInt64   `json:"checkpoints_req"`       // Number of requested checkpoints that have been performed
	CheckpointWriteTime *sql.NullFloat64 `json:"checkpoint_write_time"` // Total amount of time that has been spent in the portion of checkpoint processing
	CheckpointSyncTime  *sql.NullFloat64 `json:"checkpoint_sync_time"`  // Total amount of time that has been spent in the portion of checkpoint processing
	BuffersCheckpoint   *sql.NullInt64   `json:"buffers_checkpoint"`    // Number of buffers written during checkpoints
	BuffersClean        *sql.NullInt64   `json:"buffers_clean"`         // Number of buffers written by the background writer
	MaxWrittenClean     *sql.NullInt64   `json:"maxwritten_clean"`      // Number of times the background writer stopped a cleaning scan because it had written too many buffers
	BuffersBackend      *sql.NullInt64   `json:"buffers_backend"`       // Number of buffers written directly by a backend
	BuffersBackendFsync *sql.NullInt64   `json:"buffers_backend_fsync"` // Number of times a backend had to execute its own fsync call
	BuffersAlloc        *sql.NullInt64   `json:"buffers_alloc"`         // Number of buffers allocated
	StatsReset          *sql.NullTime    `json:"stats_reset"`           // Time at which these statistics were last reset
}

func (s *Stats) fetchBgWriter() (BgWriterView, error) {
	const query = `SELECT
	checkpoints_timed,
	checkpoints_req,
	checkpoint_write_time,
	checkpoint_sync_time,
	buffers_checkpoint,
	buffers_clean,
	maxwritten_clean,
	buffers_backend,
	buffers_backend_fsync,
	buffers_alloc,
	stats_reset
	FROM pg_stat_bgwriter`

	row := s.db.QueryRow(query)
	var res BgWriterView

	err := row.Scan(
		&res.CheckpointsTimed,
		&res.CheckpointsReq,
		&res.CheckpointWriteTime,
		&res.CheckpointSyncTime,
		&res.BuffersCheckpoint,
		&res.BuffersClean,
		&res.MaxWrittenClean,
		&res.BuffersBackend,
		&res.BuffersBackendFsync,
		&res.BuffersAlloc,
		&res.StatsReset,
	)
	return res, err
}
