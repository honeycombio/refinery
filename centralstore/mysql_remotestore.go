package centralstore

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// MySQLRemoteStore is an implementation of RemoteStorer that stores spans in a MySQL database.
type MySQLRemoteStore struct {
	db *sqlx.DB
}

type MySQLRemoteStoreOptions struct {
	DSN string
}

// NewMySQLRemoteStore creates a new MySQLRemoteStore.
func NewMySQLRemoteStore(options MySQLRemoteStoreOptions) (*MySQLRemoteStore, error) {
	db, err := sqlx.Connect("mysql", options.DSN)
	if err != nil {
		return nil, err
	}
	return &MySQLRemoteStore{db: db}, nil
}

// ensure that we implement RemoteStorer
var _ BasicStorer = (*MySQLRemoteStore)(nil)

// SQL database schema
const (
	// MySQLRemoteStoreSchema is the schema for the MySQLRemoteStore
	MySQLRemoteStoreSchema = `
CREATE TABLE IF NOT EXISTS traces (
	trace_id VARCHAR(255) PRIMARY KEY,
	trace_state ENUM('collecting', 'waiting_to_decide', 'ready_for_decision', 'awaiting_decision', 'decision_keep', 'decision_drop') NOT NULL,
	last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS spans (
	trace_id VARCHAR(255) NOT NULL,
	span_id VARCHAR(255) NOT NULL,
	parent_id VARCHAR(255),
	type ENUM('', 'link', 'span_event') NOT NULL,
	key_fields JSON,
	all_fields JSON,
	is_root BOOLEAN NOT NULL,
	primary KEY (TraceID, SpanID),
	foreign KEY (TraceID) REFERENCES traces(TraceID)
);

CREATE TABLE IF NOT EXISTS trace_status (
	trace_id VARCHAR(255) PRIMARY KEY,
	rate INT,
	keep_reason VARCHAR(255),
	last_updated TIMESTAMP
	span_count INT,
	span_event_count INT,
	span_link_count INT
);

CREATE TABLE IF NOT EXISTS dropped (
	trace_id VARCHAR(255) PRIMARY KEY,
);

CREATE INDEX IF NOT EXISTS trace_state ON traces (trace_state);
CREATE INDEX IF NOT EXISTS span_trace ON spans (trace_id);
`
)

// InitSchema initializes the MySQLRemoteStore schema.
func (m *MySQLRemoteStore) InitSchema() error {
	_, err := m.db.Exec(MySQLRemoteStoreSchema)
	return err
}

func realerror(err error) bool {
	return err != nil && err != sql.ErrNoRows
}

// WriteSpan writes a span to the store. It must always contain TraceID.
// If this is a span containing any non-empty key fields, it must also contain
// SpanID (and ParentID if it is not a root span).
// For span events and span links, it may contain only the TraceID and the SpanType field;
// these are counted but not stored.
// Root spans should always be sent and must contain at least SpanID, and have the IsRoot flag set.
// AllFields is optional and is used during shutdown.
// WriteSpan may be asynchronous and will only return an error if the span could not be written.
func (m *MySQLRemoteStore) WriteSpan(span *CentralSpan) error {
	// start a transaction
	tx, err := m.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Check the dropped table to see if we already dropped this one
	var dropped bool
	err = tx.Get(&dropped, "SELECT EXISTS(SELECT 1 FROM dropped WHERE trace_id = ?)", span.TraceID)
	if realerror(err) {
		return err
	}
	if dropped {
		// nothing else to do!
		return nil
	}

	// Fetch from the trace_status table to see if we already know about this one
	cts := CentralTraceStatus{}
	err = tx.Get(&cts, "SELECT * FROM trace_status WHERE trace_id = ?", span.TraceID)
	if realerror(err) {
		return err
	}
	// this trace is new to us, we need to create
	// a new trace_status entry for it
	if err != nil {
		stat := NewCentralTraceStatus(span.TraceID, Collecting)
		_, err = tx.NamedExec("INSERT INTO trace_status (trace_id, trace_state) VALUES (:trace_id, :trace_state)", stat)
		if err != nil {
			return err
		}
	} else {
		// we found the trace, so we need to update the counts
		cts.Count++
		if span.Type == "span_event" {
			cts.EventCount++
		} else if span.Type == "link" {
			cts.LinkCount++
		}
		_, err = tx.Exec("UPDATE kept SET span_count = ?, span_event_count = ?, span_link_count = ? WHERE trace_id = ?", cts.Count, cts.EventCount, cts.LinkCount, cts.TraceID)
		if err != nil {
			return err
		}
	}

	// it's an active trace, so we need to add the span
	_, err = tx.NamedExec("INSERT INTO spans (trace_id, span_id, parent_id, type, key_fields, all_fields, is_root) VALUES (:trace_id, :span_id, :parent_id, :type, :key_fields, :all_fields, :is_root)", span)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// GetTrace fetches the current state of a trace (including all of its
// spans) from the central store. The trace contains a list of CentralSpans,
// and these spans will usually (but not always) only contain the key
// fields. The spans returned from this call should be used for making the
// trace decision; they should not be sent as telemetry unless AllFields is
// non-null, in which case these spans should be sent if the trace decision
// is Keep. If the trace has a root span, the Root property will be
// populated. Normally this call will be made after Refinery has been asked
// to make a trace decision.
func (m *MySQLRemoteStore) GetTrace(traceID string) (*CentralTrace, error) {
	// retrieve the trace from the database
	trace := CentralTrace{
		TraceID: traceID,
	}
	spans := make([]*CentralSpan, 0)
	// get all spans from the spans table
	err := m.db.Select(&spans, "SELECT * FROM spans WHERE trace_id = ? ORDER BY is_root", traceID)
	if realerror(err) {
		return nil, err
	}
	// because of our ORDER_BY, if we have a root span it's the 0th span
	if len(spans) > 0 && spans[0].IsRoot {
		trace.Root = spans[0]
	}
	trace.Spans = spans
	return &trace, nil
}

// GetStatusForTraces returns the current state for a list of trace IDs,
// including any reason information and trace counts if the trace decision
// has been made and it was to keep the trace. If a requested trace was not
// found, it will be returned as Status:Unknown. This should be considered
// to be a bug in the central store, as the trace should have been created
// when the first span was added. Any traces with a state of DecisionKeep or
// DecisionDrop should be considered to be final and appropriately disposed
// of; the central store will not change the decision state of these traces
// after this call (although kept spans will have counts updated when late
// spans arrive).
func (m *MySQLRemoteStore) GetStatusForTraces(traceIDs []string) ([]*CentralTraceStatus, error) {
	// retrieve the trace statuses from the database for all trace IDs
	statuses := make([]*CentralTraceStatus, 0)
	err := m.db.Select(&statuses, "SELECT * FROM trace_status WHERE trace_id IN (?)", traceIDs)
	if realerror(err) {
		return nil, err
	}
	return statuses, nil
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (m *MySQLRemoteStore) GetTracesForState(state CentralTraceState) ([]string, error) {
	traceIDs := make([]string, 0)
	err := m.db.Select(&traceIDs, "SELECT trace_id FROM trace_status WHERE trace_state = ?", state)
	if realerror(err) {
		return nil, err
	}
	return traceIDs, nil
}

// ChangeTraceStatus changes the status of a set of traces from one state to another
// atomically. This can be used for all trace states except transition to Keep.
// This call updates the timestamps in the trace status.
func (m *MySQLRemoteStore) ChangeTraceStatus(traceIDs []string, fromState, toState CentralTraceState) error {
	// change the status of the traces in the database
	_, err := m.db.Exec("UPDATE trace_status SET trace_state = ? WHERE trace_id IN (?) AND trace_state = ?", toState, traceIDs, fromState)
	if realerror(err) {
		return err
	}
	return nil
}

// KeepTraces changes the status of a set of traces from AwaitingDecision to Keep;
// it is used to record the keep decisions made by the trace decision engine.
// Statuses should include Reason and Rate; do not include State as it will be ignored.
func (m *MySQLRemoteStore) KeepTraces(statuses []*CentralTraceStatus) error {
	traceIDs := make([]string, len(statuses))
	for i, status := range statuses {
		traceIDs[i] = status.TraceID
	}
	// TODO: how do we forget sent spans?
	return m.ChangeTraceStatus(traceIDs, AwaitingDecision, DecisionKeep)
}

// GetMetrics returns a map of metrics from the remote store, accumulated
// since the previous time this method was called.
func (m *MySQLRemoteStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}
