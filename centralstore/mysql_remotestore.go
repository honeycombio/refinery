package centralstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type MysqlTraceStatus struct {
	TraceID    string            `db:"trace_id"`
	State      CentralTraceState `db:"state"`
	Rate       uint              `db:"rate"`
	KeepReason string            `db:"keep_reason"`
	Timestamp  uint64            `db:"last_updated"`
	Count      uint32            `db:"span_count"`
	EventCount uint32            `db:"span_event_count"`
	LinkCount  uint32            `db:"span_link_count"`
}

func (m *MysqlTraceStatus) ToCTS() *CentralTraceStatus {
	ts := time.Unix(int64(m.Timestamp), 0)
	return &CentralTraceStatus{
		TraceID:    m.TraceID,
		State:      m.State,
		Rate:       m.Rate,
		KeepReason: m.KeepReason,
		Timestamp:  ts,
		Count:      m.Count,
		EventCount: m.EventCount,
		LinkCount:  m.LinkCount,
	}
}

func GetMTS(c *CentralTraceStatus) *MysqlTraceStatus {
	ts := uint64(c.Timestamp.Unix())
	return &MysqlTraceStatus{
		TraceID:    c.TraceID,
		State:      c.State,
		Rate:       c.Rate,
		KeepReason: c.KeepReason,
		Timestamp:  ts,
		Count:      c.Count,
		EventCount: c.EventCount,
		LinkCount:  c.LinkCount,
	}
}

type MysqlCentralSpan struct {
	TraceID   string   `db:"trace_id"`
	SpanID    string   `db:"span_id"`
	ParentID  string   `db:"parent_id"`
	Type      SpanType `db:"type"`
	KeyFields []byte   `db:"key_fields"`
	AllFields []byte   `db:"all_fields"`
	IsRoot    bool     `db:"is_root"`
}

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

func (m *MySQLRemoteStore) mustExec(cmd string) {
	_, err := m.db.Exec(cmd)
	if err != nil {
		panic(err)
	}
}

func (m *MySQLRemoteStore) SetupDatabase() {
	m.mustExec(`
		CREATE TABLE IF NOT EXISTS traces (
			trace_id VARCHAR(255) PRIMARY KEY,
			trace_state ENUM('collecting', 'waiting_to_decide', 'ready_for_decision', 'awaiting_decision', 'decision_keep', 'decision_drop') NOT NULL,
			last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	);`)

	m.mustExec(`
		CREATE TABLE IF NOT EXISTS spans (
			trace_id VARCHAR(255) NOT NULL,
			span_id VARCHAR(255) NOT NULL,
			parent_id VARCHAR(255),
			type ENUM('', 'link', 'span_event') NOT NULL,
			key_fields JSON,
			all_fields JSON,
			is_root BOOLEAN NOT NULL,
			primary KEY (trace_id, span_id),
			foreign KEY (trace_id) REFERENCES traces(trace_id)
	);`)

	m.mustExec(`
		CREATE TABLE IF NOT EXISTS trace_status (
			trace_id VARCHAR(255) PRIMARY KEY,
			state ENUM('collecting', 'waiting_to_decide', 'ready_for_decision', 'awaiting_decision', 'decision_keep', 'decision_drop') NOT NULL,
			rate INT NOT NULL DEFAULT 1,
			keep_reason VARCHAR(255) NOT NULL DEFAULT "",
			last_updated INT NOT NULL DEFAULT 0,
			span_count INT NOT NULL DEFAULT 0,
			span_event_count INT NOT NULL DEFAULT 0,
			span_link_count INT NOT NULL DEFAULT 0,
			foreign KEY (trace_id) REFERENCES traces(trace_id)
	);`)

	m.mustExec(`
		CREATE TABLE IF NOT EXISTS dropped (
			trace_id VARCHAR(255) PRIMARY KEY
	);`)

	m.mustExec(`CREATE INDEX trace_state ON traces (trace_state);`)
	m.mustExec(`CREATE INDEX span_trace ON spans (trace_id);`)
}

func (m *MySQLRemoteStore) Stop() error {
	return m.db.Close()
}

func (m *MySQLRemoteStore) DeleteAllData() {
	m.mustExec("DROP TABLE IF EXISTS spans")
	m.mustExec("DROP TABLE IF EXISTS trace_status")
	m.mustExec("DROP TABLE IF EXISTS dropped")
	m.mustExec("DROP TABLE IF EXISTS traces")
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
	err = tx.Get(&dropped, "SELECT EXISTS(SELECT true FROM dropped WHERE trace_id = ?)", span.TraceID)
	if realerror(err) {
		return err
	}
	if dropped {
		// nothing else to do!
		return nil
	}

	// Fetch from the trace_status table to see if we already know about this one
	mts := MysqlTraceStatus{}
	err = tx.Get(&mts, "SELECT * FROM trace_status WHERE trace_id = ?", span.TraceID)
	if realerror(err) {
		return err
	}
	cts := mts.ToCTS()
	// this trace is new to us, we need to create
	// a new traces entry for it
	if err != nil {
		stat := NewCentralTraceStatus(span.TraceID, Collecting)
		_, err = tx.NamedExec("INSERT INTO traces (trace_id, trace_state) VALUES (:trace_id, :state)", stat)
		if err != nil {
			return err
		}
		_, err = tx.NamedExec("INSERT INTO trace_status (trace_id) VALUES (:trace_id)", stat)
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
		_, err = tx.Exec("UPDATE trace_status SET span_count = ?, span_event_count = ?, span_link_count = ? WHERE trace_id = ?", cts.Count, cts.EventCount, cts.LinkCount, cts.TraceID)
		if err != nil {
			return err
		}
	}

	// it's an active trace, so we need to add the span
	keyfields, err := json.Marshal(span.KeyFields)
	if err != nil {
		return err
	}
	allfields, err := json.Marshal(span.AllFields)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
		INSERT INTO spans (trace_id, span_id, parent_id, type, key_fields, all_fields, is_root)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		span.TraceID, span.SpanID, span.ParentID, span.Type, keyfields, allfields, span.IsRoot)
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
	spans := make([]*MysqlCentralSpan, 0)
	// get all spans from the spans table
	err := m.db.Select(&spans, "SELECT * FROM spans WHERE trace_id = ? ORDER BY is_root", traceID)
	if realerror(err) {
		return nil, err
	}
	// convert the spans to CentralSpans
	for _, mspan := range spans {
		span := CentralSpan{
			TraceID:   mspan.TraceID,
			SpanID:    mspan.SpanID,
			ParentID:  mspan.ParentID,
			Type:      mspan.Type,
			KeyFields: make(map[string]interface{}),
			AllFields: make(map[string]interface{}),
			IsRoot:    mspan.IsRoot,
		}
		err = json.Unmarshal(mspan.KeyFields, &span.KeyFields)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(mspan.AllFields, &span.AllFields)
		if err != nil {
			return nil, err
		}
		trace.Spans = append(trace.Spans, &span)
	}

	// because of our ORDER_BY, if we have a root span it's the 0th span
	if len(spans) > 0 && spans[0].IsRoot {
		trace.Root = trace.Spans[0]
	}
	return &trace, nil
}

func generateInClause(sa []string) string {
	return "('" + strings.Join(sa, "', '") + "')"
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
	mstatuses := make([]*MysqlTraceStatus, 0)
	query := "SELECT * FROM trace_status WHERE trace_id IN " + generateInClause(traceIDs)
	fmt.Println(query)
	err := m.db.Select(&mstatuses, query)
	if realerror(err) {
		return nil, err
	}
	if len(mstatuses) == 0 {
		return nil, nil
	}
	cstatuses := make([]*CentralTraceStatus, len(mstatuses))
	for i, mstatus := range mstatuses {
		cstatuses[i] = mstatus.ToCTS()
	}
	// fmt.Printf("GetStatusForTraces: %v\n", cstatuses)
	return cstatuses, nil
}

// GetTracesForState returns a list of trace IDs that match the provided status.
func (m *MySQLRemoteStore) GetTracesForState(state CentralTraceState) ([]string, error) {
	traceIDs := make([]string, 0)
	err := m.db.Select(&traceIDs, "SELECT trace_id FROM trace_status WHERE state = ?", state)
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
	tids := strings.Join(traceIDs, ", ")
	fmt.Printf("changing trace status from %s to %s for %s\n", fromState, toState, tids)
	_, err := m.db.Exec(
		"UPDATE trace_status SET state = ? WHERE state = ? AND trace_id IN "+generateInClause(traceIDs),
		toState, fromState)
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
