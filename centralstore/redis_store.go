package centralstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

func NewRedisRemoteStore(host string) *RedisRemoteStore {
	if host == "" {
		host = "localhost:6379"
	}

	redisConn := &redisConn{
		pool: newPool(host),
	}
	return &RedisRemoteStore{
		client: redisConn,
		traces: NewTraceStore(redisConn),
		statuses: map[CentralTraceState]*TraceStateMap{
			Collecting:       NewTraceStateMap(redisConn, Collecting),
			WaitingToDecide:  NewTraceStateMap(redisConn, WaitingToDecide),
			ReadyForDecision: NewTraceStateMap(redisConn, ReadyForDecision),
			AwaitingDecision: NewTraceStateMap(redisConn, AwaitingDecision),
			DecisionDrop:     NewTraceStateMap(redisConn, DecisionDrop),
			DecisionKeep:     NewTraceStateMap(redisConn, DecisionKeep),
		},
	}
}

var _ RemoteStore = (*RedisRemoteStore)(nil)

// RedisRemoteStore is an implementation of RemoteStore.
type RedisRemoteStore struct {
	client   *redisConn
	traces   *TracesStore
	statuses map[CentralTraceState]*TraceStateMap
}

func (r *RedisRemoteStore) Close() error {
	return r.client.pool.Close()
}

func (r *RedisRemoteStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}

// start a redis transaction
func (r *RedisRemoteStore) Exec(func() error) error {
	return nil
}

func (r *RedisRemoteStore) WriteSpan(span *CentralSpan) error {
	if r.statuses[DecisionDrop].Exists(span.TraceID) {
		return nil
	}

	err := r.traces.AddSpan(span)
	if err != nil {
		return err
	}

	isNewTrace := true
	if r.statuses[DecisionKeep].Exists(span.TraceID) || r.statuses[WaitingToDecide].Exists(span.TraceID) {
		isNewTrace = false
		// late span
	}

	collectingSet := r.statuses[Collecting]

	if collectingSet.Exists(span.TraceID) {
		isNewTrace = false
		if span.ParentID == "" {
			err := collectingSet.Move(span.TraceID, r.statuses[WaitingToDecide])
			if err != nil {
				return err
			}
		}

	}

	// if the span belongs to a new trace, add it to the COLLECTING state and add the trace to the traces map
	if isNewTrace {
		err := collectingSet.AddTrace(span.TraceID)
		if err != nil {
			return err
		}
		err = r.traces.AddTrace(span)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RedisRemoteStore) GetTrace(traceID string) (*CentralTrace, error) {
	return r.traces.GetTrace(traceID)
}

func (r *RedisRemoteStore) GetStatusForTraces(traceIDs []string) ([]*CentralTraceStatus, error) {
	statusesMap, err := r.traces.GetTraceStatuses(traceIDs)
	if err != nil {
		return nil, err
	}

	remaining := traceIDs
	for state, status := range r.statuses {
		if len(remaining) == 0 {
			break
		}

		exist, notExist := status.ExistAll(remaining)
		timestamps, err := status.GetTimestampForTraces(exist)
		if err != nil {
			return nil, err
		}
		for _, id := range exist {
			_, ok := statusesMap[id]
			if !ok {
				continue
			}
			statusesMap[id].State = state
			statusesMap[id].timestamp = timestamps[id]
		}

		remaining = notExist
	}

	for _, id := range remaining {
		_, ok := statusesMap[id]
		if !ok {
			continue
		}
		statusesMap[id].State = Unknown

	}

	statuses := make([]*CentralTraceStatus, 0, len(statusesMap))
	for _, status := range statusesMap {
		statuses = append(statuses, status)
	}

	return statuses, nil

}

func (r *RedisRemoteStore) GetTracesForState(state CentralTraceState) ([]string, error) {
	return r.statuses[state].GetAllTraces(), nil
}

func (r *RedisRemoteStore) ChangeTraceStatus(traceIDs []string, fromState, toState CentralTraceState) error {
	var errs []error
	for _, traceID := range traceIDs {
		err := r.changeTraceStatus(traceID, fromState, toState)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (r *RedisRemoteStore) KeepTraces(statuses []*CentralTraceStatus) error {
	var errs []error
	for _, status := range statuses {
		err := r.statuses[AwaitingDecision].Move(status.TraceID, r.statuses[DecisionKeep])
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (r *RedisRemoteStore) changeTraceStatus(traceID string, fromState, toState CentralTraceState) error {
	err := r.statuses[fromState].Move(traceID, r.statuses[toState])
	if err != nil {
		return err
	}

	return nil
}

type centralTraceStatusHash struct {
	TraceID        string `redis:"trace_id"`
	Rate           uint
	KeepReason     string `redis:"keep_reason"`
	ReasonIndex    uint   `redis:"reason_index"`
	SpanCount      uint32 `redis:"span_count"`
	SpanEventCount uint32 `redis:"span_event_count"`
	SpanLinkCount  uint32 `redis:"span_link_count"`
}

func (c *centralTraceStatusHash) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"trace_id":         c.TraceID,
		"rate":             c.Rate,
		"keep_reason":      c.KeepReason,
		"reason_index":     c.ReasonIndex,
		"span_count":       c.SpanCount,
		"span_event_count": c.SpanEventCount,
		"span_link_count":  c.SpanLinkCount,
	}
}

func newCentralTraceStatusHash(status *CentralTraceStatus) *centralTraceStatusHash {
	return &centralTraceStatusHash{
		TraceID:        status.TraceID,
		Rate:           status.Rate,
		KeepReason:     status.KeepReason,
		ReasonIndex:    status.reasonIndex,
		SpanCount:      status.spanCount,
		SpanEventCount: status.spanEventCount,
		SpanLinkCount:  status.spanLinkCount,
	}
}

// TraceStore stores trace state status and spans.
// trace state statuses is stored in a redis hash with the key being the trace ID
// and each field being a status field.
// for example, an entry in the hash would be: "trace1:trace" -> "state:DecisionKeep, rate:100, reason:reason1"
// spans are stored in a redis hash with the key being the trace ID and each field being a span ID and the value being the serialized CentralSpan.
// for example, an entry in the hash would be: "trace1:spans" -> "span1:{spanID:span1, KeyFields: []}, span2:{spanID:span2, KeyFields: []}"
type TracesStore struct {
	traces RedisHasher // trace ID -> CentralTraceStateStatus
	spans  *SpansList
}

func NewTraceStore(redis *redisConn) *TracesStore {
	traces := NewRedisHash(redis)
	spans := NewSpanList(NewRedisHash(redis))
	return &TracesStore{
		traces: traces,
		spans:  spans,
	}
}

func (t *TracesStore) AddTrace(span *CentralSpan) error {
	trace := &CentralTraceStatus{
		TraceID: span.TraceID,
	}

	err := t.traces.HMSET(fmt.Sprintf("%s:trace", trace.TraceID), newCentralTraceStatusHash(trace).ToMap())
	if err != nil {
		return err
	}

	return t.spans.AddSpan(span)
}

func (t *TracesStore) GetTrace(traceID string) (*CentralTrace, error) {
	spans, rootSpan, err := t.getSpans(traceID)
	if err != nil {
		return nil, err
	}

	return &CentralTrace{
		TraceID: traceID,
		Root:    rootSpan,
		Spans:   spans,
	}, nil
}

func (t *TracesStore) GetTraceStatuses(traceIDs []string) (map[string]*CentralTraceStatus, error) {
	statuses := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, traceID := range traceIDs {
		status := &centralTraceStatusHash{}
		err := t.traces.HGETALLStruct(t.traceStateKey(traceID), status)
		if err != nil {
			return nil, err
		}

		statuses[traceID] = &CentralTraceStatus{
			TraceID:        status.TraceID,
			State:          Unknown,
			Rate:           status.Rate,
			KeepReason:     status.KeepReason,
			reasonIndex:    status.ReasonIndex,
			spanCount:      status.SpanCount,
			spanEventCount: status.SpanEventCount,
			spanLinkCount:  status.SpanLinkCount,
		}
	}

	if len(statuses) == 0 {
		return nil, fmt.Errorf("no trace found")

	}

	return statuses, nil
}

func (t *TracesStore) getSpans(traceID string) ([]*CentralSpan, *CentralSpan, error) {
	return t.spans.GetAll(traceID)
}

func (t *TracesStore) SetValue(traceID string, field string, value interface{}) error {
	return t.traces.HSET(t.traceStateKey(traceID), field, value)
}

func (t *TracesStore) traceStateKey(traceID string) string {
	return fmt.Sprintf("%s:trace", traceID)
}

func (t *TracesStore) AddSpan(span *CentralSpan) error {
	return t.spans.AddSpan(span)
}

// SpansList stores spans in a redis hash with the key being the trace ID and each field
// being a span ID and the value being the serialized CentralSpan.
type SpansList struct {
	spans RedisHasher
}

func NewSpanList(spans RedisHasher) *SpansList {
	return &SpansList{
		spans: spans,
	}
}

// trace ID -> a hash of span ID(field)/central span(value)

func (s *SpansList) spansKey(traceID string) string {
	return fmt.Sprintf("%s:spans", traceID)
}

// central span -> blobs
func (s *SpansList) AddSpan(span *CentralSpan) error {
	data, err := s.Serialize(span)
	if err != nil {
		return err
	}

	// overwrite the span data if it already exists
	return s.spans.HSET(s.spansKey(span.TraceID), span.SpanID, data)
}

func (s *SpansList) Serialize(span *CentralSpan) ([]byte, error) {
	return json.Marshal(span)
}

func (s *SpansList) Deserialize(data []byte) (*CentralSpan, error) {
	span := &CentralSpan{}
	err := json.Unmarshal(data, span)
	return span, err
}

func (s *SpansList) GetAll(traceID string) ([]*CentralSpan, *CentralSpan, error) {
	spans := make([]*CentralSpan, 0)
	var tmpSpan []struct {
		SpanID string
		Span   []byte
	}

	err := s.spans.HGETALLSlice(s.spansKey(traceID), &tmpSpan)
	if err != nil {
		return spans, nil, err
	}

	var rootSpan *CentralSpan
	for _, d := range tmpSpan {
		span, err := s.Deserialize(d.Span)
		if err != nil {
			continue
		}
		spans = append(spans, span)

		if span.ParentID == "" {
			rootSpan = span
		}
	}
	return spans, rootSpan, nil
}

// TraceStateMap is a map of trace IDs to their state.
// It's used to atomically move traces between states and to get all traces in a state.
// In order to also record the time of the state change, we also store traceID:timestamp
// in a hash using <state>:traces:time as the hash key.
// By using a hash for the timestamps, we can easily get the timestamp for a trace based
// on it's current state.
type TraceStateMap struct {
	traces                RedisMapper
	stateChangeTimestamps RedisHasher
	state                 CentralTraceState
}

func NewTraceStateMap(conn *redisConn, state CentralTraceState) *TraceStateMap {
	return &TraceStateMap{
		traces:                NewRedisMap(conn),
		stateChangeTimestamps: NewRedisHash(conn),
		state:                 state,
	}
}

func (t *TraceStateMap) AddTrace(traceID string) error {
	// TODO: do this in a transaction
	err := t.traces.SADD(t.MapKey(), traceID)
	if err != nil {
		return err
	}

	return t.updateStateChangeTimestamp(traceID, t.state)
}

func (t *TraceStateMap) MapKey() string {
	return fmt.Sprintf("%s:traces", t.state)
}

func (t *TraceStateMap) GetAllTraces() []string {
	results := t.traces.SMEMBERS(t.MapKey())

	traces := make([]string, 0, len(results))
	for _, r := range results {
		trace, err := redis.String(r, nil)
		if err != nil {
			return nil
		}
		traces = append(traces, trace)
	}
	return traces
}

func (t *TraceStateMap) Exists(traceID string) bool {
	return t.traces.SISMEMBER(t.MapKey(), traceID)
}

func (t *TraceStateMap) ExistAll(traceIDs []string) ([]string, []string) {
	if len(traceIDs) == 0 {
		return nil, nil
	}

	data := make([]interface{}, len(traceIDs))
	for i, traceID := range traceIDs {
		data[i] = traceID
	}
	// 0 means it doesn't exist, 1 means it does
	exist := make([]string, 0, len(traceIDs))
	notexist := make([]string, 0, len(traceIDs))
	for i, result := range t.traces.SMISMEMBER(t.MapKey(), data...) {
		switch result.(int64) {
		case 0:
			notexist = append(notexist, traceIDs[i])
		case 1:
			exist = append(exist, traceIDs[i])
		}
	}

	return exist, notexist
}

func (t *TraceStateMap) Remove(traceID string) error {
	return t.traces.SREM(t.MapKey(), traceID)
}

func (t *TraceStateMap) Move(traceID string, destination *TraceStateMap) error {
	err := t.traces.SMOVE(t.MapKey(), destination.MapKey(), traceID)
	if err != nil {
		return err
	}

	return t.updateStateChangeTimestamp(traceID, destination.state)
}

func (t *TraceStateMap) GetTimestampForTraces(traceIDs []string) (map[string]time.Time, error) {
	var timestamps []struct {
		TraceID   string
		Timestamp int64
	}
	err := t.stateChangeTimestamps.HGETALLSlice(t.stateChangeTimestampMapKey(string(t.state)), &timestamps)
	if err != nil {
		return nil, err
	}

	results := make(map[string]time.Time, len(timestamps))
	for _, t := range timestamps {
		results[t.TraceID] = time.Unix(t.Timestamp, 0)
	}
	return results, nil
}

func (t *TraceStateMap) updateStateChangeTimestamp(traceID string, state CentralTraceState) error {
	// TODO: We could change the key to be a timerange so that we can set ttl for the key. This will allow us to
	// do automatic cleanup of the stateChangeTimestamps
	if _, err := t.stateChangeTimestamps.HDEL(t.stateChangeTimestampMapKey(string(t.state)), traceID); err != nil {
		return err
	}

	return t.stateChangeTimestamps.HSET(t.stateChangeTimestampMapKey(string(state)), traceID, time.Now().Unix())
}

func (t *TraceStateMap) stateChangeTimestampMapKey(state string) string {
	return fmt.Sprintf("%s:traces:time", state)
}

type RedisMapper interface {
	SADD(key string, member interface{}) error                   //adds one or more new member to a set.
	SREM(key string, member interface{}) error                   //removes the specified member from the set.
	SISMEMBER(key string, member interface{}) bool               // tests a string for set membership.
	SMISMEMBER(key string, members ...interface{}) []interface{} // tests multiple strings for set membership.
	SMEMBERS(key string) []interface{}                           // returns all the members of the set value stored at key.
	SMOVE(source, destination string, member interface{}) error  // moves a member from one set to another atomically.
}

type RedisMap struct {
	client *redisConn
}

func NewRedisMap(conn *redisConn) *RedisMap {
	return &RedisMap{
		client: conn,
	}
}

func (r *RedisMap) SADD(key string, member interface{}) error {
	_, err := r.client.Do("SADD", key, member)
	return err
}

func (r *RedisMap) SISMEMBER(key string, member interface{}) bool {
	result, err := redis.Int64(r.client.Do("SISMEMBER", key, member))
	if err != nil {
		return false
	}

	return result == 1
}

func (r *RedisMap) SMISMEMBER(key string, members ...interface{}) []interface{} {
	args := make([]interface{}, 0, len(members)+1)
	args = append(args, key)
	args = append(args, members...)
	result, err := redis.Values(r.client.Do("SMISMEMBER", args...))
	if err != nil {
		return nil
	}

	return result
}

func (r *RedisMap) SMOVE(source, destination string, member interface{}) error {
	args := make([]interface{}, 0, 3)
	args = append(args, source)
	args = append(args, destination)
	args = append(args, member)
	_, err := r.client.Do("SMOVE", args...)
	return err
}

func (r *RedisMap) SMEMBERS(key string) []interface{} {
	result, err := redis.Values(r.client.Do("SMEMBERS", key))
	if err != nil {
		return nil
	}

	return result
}

func (r *RedisMap) SREM(key string, member interface{}) error {
	_, err := r.client.Do("SREM", key, member)
	return err
}

type RedisHasher interface {
	HSET(key string, field string, value interface{}) error // sets field in the hash stored at key to value.
	HGET(key string, field string) (interface{}, error)     // returns the value associated with field in the hash stored at key.
	HDEL(key string, field string) (bool, error)            // removes the specified fields from the hash stored at key.
	HMSET(key string, fields map[string]interface{}) error  // sets the specified fields to their respective values in the hash stored at key.
	HGETALL(key string) ([]interface{}, error)              // returns all fields and values of the hash stored at key.
	HGETALLStruct(key string, out interface{}) error        // returns all fields and values of the hash stored at key.
	HGETALLSlice(key string, out interface{}) error         // returns all fields and values of the hash stored at key.
}

type RedisHash struct {
	client *redisConn
}

func NewRedisHash(conn *redisConn) *RedisHash {
	return &RedisHash{
		client: conn,
	}
}

func (r *RedisHash) HSET(key string, field string, value interface{}) error {
	_, err := r.client.Do("HSET", key, field, value)
	return err
}

func (r *RedisHash) HGET(key string, field string) (interface{}, error) {
	return r.client.Do("HGET", key, field)
}

func (r *RedisHash) HDEL(key string, field string) (bool, error) {
	return redis.Bool(r.client.Do("HDEL", key, field))
}

func (r *RedisHash) HMSET(key string, fields map[string]interface{}) error {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for k, v := range fields {
		args = append(args, k)
		args = append(args, v)
	}
	_, err := r.client.Do("HMSET", args...)
	return err
}

func (r *RedisHash) HGETALL(key string) ([]interface{}, error) {
	return redis.Values(r.client.Do("HGETALL", key))
}

func (r *RedisHash) HGETALLStruct(key string, out interface{}) error {
	result, err := r.HGETALL(key)
	if err != nil {
		return err
	}
	err = redis.ScanStruct(result, out)
	if err != nil {
		return fmt.Errorf("failed to get values for key %s with error %s ", key, err)
	}
	return nil
}

func (r *RedisHash) HGETALLSlice(key string, out interface{}) error {
	result, err := r.HGETALL(key)
	if err != nil {
		return err
	}

	err = redis.ScanSlice(result, out)
	if err != nil {
		return fmt.Errorf("failed to get values for key %s with error %s ", key, err)
	}
	return nil
}

func newPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

type redisConn struct {
	pool *redis.Pool
}

func (r *redisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	conn := r.pool.Get()
	defer conn.Close()

	return conn.Do(commandName, args...)
}
