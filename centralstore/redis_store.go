package centralstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

var _ BasicStorer = (*RedisRemoteStore)(nil)

// TODO:
// - wrap the redis commands in a transaction so that we can ensure that the state change and the trace move are atomic

func NewRedisRemoteStore(host string) *RedisRemoteStore {
	if host == "" {
		host = "localhost:6379"
	}

	redisConn := &redisConn{
		pool: newPool(host),
	}

	decisionCache, err := cache.NewCuckooSentCache(config.SampleCacheConfig{}, &metrics.NullMetrics{})
	if err != nil {
		panic(err)
	}
	return &RedisRemoteStore{
		client:        redisConn,
		decisionCache: decisionCache,
		traces:        newTraceStore(redisConn),
		states: map[CentralTraceState]*traceStateMap{
			Collecting:       newTraceStateMap(redisConn, Collecting),
			WaitingToDecide:  newTraceStateMap(redisConn, WaitingToDecide),
			ReadyForDecision: newTraceStateMap(redisConn, ReadyForDecision),
			AwaitingDecision: newTraceStateMap(redisConn, AwaitingDecision),
		},
	}
}

// RedisRemoteStore is an implementation of RemoteStore.
type RedisRemoteStore struct {
	client        *redisConn
	decisionCache cache.TraceSentCache
	traces        *traceStore
	states        map[CentralTraceState]*traceStateMap
}

func (r *RedisRemoteStore) Close() error {
	return r.client.pool.Close()
}

func (r *RedisRemoteStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}

func (r *RedisRemoteStore) findTraceStatus(traceID string) (CentralTraceState, *CentralTraceStatus) {
	if tracerec, _, found := r.decisionCache.Test(traceID); found {
		// it was in the decision cache, so we can return the right thing
		if tracerec.Kept() {
			return DecisionKeep, NewCentralTraceStatus(traceID, DecisionKeep)
		} else {
			return DecisionDrop, NewCentralTraceStatus(traceID, DecisionDrop)
		}
	}

	if r.states[DecisionDrop].exists(traceID) {
		return DecisionDrop, NewCentralTraceStatus(traceID, DecisionDrop)
	}

	if r.states[DecisionKeep].exists(traceID) {
		return DecisionKeep, NewCentralTraceStatus(traceID, DecisionKeep)
	}
	if r.states[WaitingToDecide].exists(traceID) {
		return WaitingToDecide, NewCentralTraceStatus(traceID, WaitingToDecide)
	}

	if r.states[Collecting].exists(traceID) {
		return Collecting, NewCentralTraceStatus(traceID, Collecting)
	}

	return Unknown, nil
}

func (r *RedisRemoteStore) WriteSpan(span *CentralSpan) error {
	state, _ := r.findTraceStatus(span.TraceID)

	switch state {
	case DecisionDrop:
		return nil
	case DecisionKeep, AwaitingDecision:
	// updated:= r.trace.UpdateSpanCount(span)
	// r.decesionCache.Check(updated)
	// mark as late span
	case Collecting:
		if span.ParentID == "" {
			err := r.states[Collecting].move(span.TraceID, r.states[WaitingToDecide])
			if err != nil {
				return err
			}
		}
	case WaitingToDecide, ReadyForDecision:
	case Unknown:
		err := r.states[Collecting].addTrace(span.TraceID)
		if err != nil {
			return err
		}
		err = r.traces.addTrace(span)
		if err != nil {
			return err
		}
	}

	err := r.traces.addSpan(span)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisRemoteStore) GetTrace(traceID string) (*CentralTrace, error) {
	return r.traces.getTrace(traceID)
}

func (r *RedisRemoteStore) GetStatusForTraces(traceIDs []string) ([]*CentralTraceStatus, error) {
	statusesMap, err := r.traces.getTraceStatuses(traceIDs)
	if err != nil {
		return nil, err
	}

	remaining := traceIDs
	for state, status := range r.states {
		if len(remaining) == 0 {
			break
		}

		exist, err := status.getTraces(remaining)
		if err != nil {
			return nil, err
		}
		notExist := make([]string, 0, len(remaining))
		for id, timestamp := range exist {
			if timestamp.IsZero() {
				notExist = append(notExist, id)
				continue
			}
			_, ok := statusesMap[id]
			if !ok {
				continue
			}
			statusesMap[id].State = state
			statusesMap[id].timestamp = timestamp
		}

		remaining = notExist
	}

	for _, id := range remaining {
		_, ok := statusesMap[id]
		if !ok {
			continue
		}
		if tracerec, _, found := r.decisionCache.Test(id); found {
			// it was in the decision cache, so we can return the right thing
			if tracerec.Kept() {
				statusesMap[id].State = DecisionKeep
			} else {
				statusesMap[id].State = DecisionDrop
			}
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
	switch state {
	case DecisionDrop, DecisionKeep:
		return nil, nil
	}
	return r.states[state].getAllTraces()
}

func (r *RedisRemoteStore) ChangeTraceStatus(traceIDs []string, fromState, toState CentralTraceState) error {
	if toState == DecisionDrop {
		traces, err := r.traces.getTraceStatuses(traceIDs)
		if err != nil {
			return err
		}

		for _, trace := range traces {
			r.decisionCache.Record(trace, false, "")
		}

		return r.removeTraces(traceIDs)
	}

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
		err := r.changeTraceStatus(status.TraceID, AwaitingDecision, DecisionKeep)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (r *RedisRemoteStore) removeTraces(traceIDs []string) error {
	err := r.traces.remove(traceIDs)
	if err != nil {
		return err
	}

	return r.states[AwaitingDecision].remove(traceIDs...)
}

func (r *RedisRemoteStore) changeTraceStatus(traceID string, fromState, toState CentralTraceState) error {
	err := r.states[fromState].move(traceID, r.states[toState])
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
type traceStore struct {
	traces redisHasher // trace ID -> CentralTraceStateStatus
	spans  *spansList
}

func newTraceStore(redis *redisConn) *traceStore {
	traces := newRedisHash(redis)
	spans := NewSpanList(newRedisHash(redis))
	return &traceStore{
		traces: traces,
		spans:  spans,
	}
}

func (t *traceStore) addTrace(span *CentralSpan) error {
	trace := &CentralTraceStatus{
		TraceID: span.TraceID,
	}

	err := t.traces.mset(t.traceStateKey(span.TraceID), newCentralTraceStatusHash(trace).ToMap())
	if err != nil {
		return err
	}

	return t.spans.addSpan(span)
}

func (t *traceStore) getTrace(traceID string) (*CentralTrace, error) {
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

func (t *traceStore) getTraceStatuses(traceIDs []string) (map[string]*CentralTraceStatus, error) {
	statuses := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, traceID := range traceIDs {
		status := &centralTraceStatusHash{}
		err := t.traces.getAllStruct(t.traceStateKey(traceID), status)
		if err != nil {
			return nil, err
		}
		// TODO: we need to store the current span count in the trace status
		count, err := t.spans.count(traceID)
		if err != nil {
			return nil, err
		}

		statuses[traceID] = &CentralTraceStatus{
			TraceID:        status.TraceID,
			State:          Unknown,
			Rate:           status.Rate,
			KeepReason:     status.KeepReason,
			reasonIndex:    status.ReasonIndex,
			spanCount:      uint32(count.spanCount),
			spanEventCount: uint32(count.spanEventCount),
			spanLinkCount:  uint32(count.spanLinkCount),
		}
	}

	if len(statuses) == 0 {
		return nil, fmt.Errorf("no trace found")

	}

	return statuses, nil
}

func (t *traceStore) getSpans(traceID string) ([]*CentralSpan, *CentralSpan, error) {
	return t.spans.getAll(traceID)
}

func (t *traceStore) traceStateKey(traceID string) string {
	return fmt.Sprintf("%s:trace", traceID)
}

func (t *traceStore) addSpan(span *CentralSpan) error {
	return t.spans.addSpan(span)
}

func (t *traceStore) remove(traceIDs []string) error {
	// remove span list
	err := t.spans.remove(traceIDs)
	if err != nil {
		return err
	}
	// remove trace status
	return t.traces.del(traceIDs...)
}

// spansList stores spans in a redis hash with the key being the trace ID and each field
// being a span ID and the value being the serialized CentralSpan.
type spansList struct {
	spans redisHasher
}

func NewSpanList(spans redisHasher) *spansList {
	return &spansList{
		spans: spans,
	}
}

// trace ID -> a hash of span ID(field)/central span(value)

func (s *spansList) spansKey(traceID string) string {
	return fmt.Sprintf("%s:spans", traceID)
}

func (s *spansList) spansField(spanID string, spanType string) string {
	if spanType == "" {
		return spanID
	}
	return fmt.Sprintf("%s:%s", spanID, spanType)
}

// central span -> blobs
func (s *spansList) addSpan(span *CentralSpan) error {
	data, err := s.serialize(span)
	if err != nil {
		return err
	}

	// overwrite the span data if it already exists
	return s.spans.set(s.spansKey(span.TraceID), s.spansField(span.SpanID, string(span.Type)), data)
}

func (s *spansList) serialize(span *CentralSpan) ([]byte, error) {
	return json.Marshal(span)
}

func (s *spansList) deserialize(data []byte) (*CentralSpan, error) {
	span := &CentralSpan{}
	err := json.Unmarshal(data, span)
	return span, err
}

func (s *spansList) getAll(traceID string) ([]*CentralSpan, *CentralSpan, error) {
	spans := make([]*CentralSpan, 0)
	var tmpSpan []struct {
		SpanID string
		Span   []byte
	}

	err := s.spans.getAllSlice(s.spansKey(traceID), &tmpSpan)
	if err != nil {
		return spans, nil, err
	}

	var rootSpan *CentralSpan
	for _, d := range tmpSpan {
		span, err := s.deserialize(d.Span)
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

type spanCounts struct {
	spanCount      int
	spanEventCount int
	spanLinkCount  int
}

func (s *spansList) count(traceID string) (spanCounts, error) {
	values, err := redis.Strings(s.spans.getAllFields(s.spansKey(traceID)))
	if err != nil {
		return spanCounts{}, err
	}

	counts := spanCounts{
		spanCount: len(values),
	}
	for _, v := range values {
		if strings.Contains(v, "link") {
			counts.spanLinkCount++
		}
		if strings.Contains(v, "event") {
			counts.spanEventCount++
		}
	}

	return counts, nil
}

func (s *spansList) remove(traceIDs []string) error {
	return s.spans.del(traceIDs...)
}

// traceStateMap is a map of trace IDs to their state.
// It's used to atomically move traces between states and to get all traces in a state.
// In order to also record the time of the state change, we also store traceID:timestamp
// in a hash using <state>:traces:time as the hash key.
// By using a hash for the timestamps, we can easily get the timestamp for a trace based
// on it's current state.
type traceStateMap struct {
	traces *redisSortedSet
	state  CentralTraceState
}

func newTraceStateMap(conn *redisConn, state CentralTraceState) *traceStateMap {
	return &traceStateMap{
		traces: &redisSortedSet{client: conn},
		state:  state,
	}
}

func (t *traceStateMap) addTrace(traceID string) error {
	err := t.traces.add(t.mapKey(), float64(time.Now().Unix()), traceID)
	if err != nil {
		return err
	}

	return nil
}

func (t *traceStateMap) mapKey() string {
	return fmt.Sprintf("%s:traces", t.state)
}

func (t *traceStateMap) getAllTraces() ([]string, error) {
	results, err := t.traces.getAllFields(t.mapKey())
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (t *traceStateMap) getTraces(traceIDs []string) (map[string]time.Time, error) {
	exist, err := t.traces.getScores(t.mapKey(), traceIDs)
	if err != nil {
		return nil, err
	}

	traces := make(map[string]time.Time, len(exist))
	for i, value := range exist {
		if value == nil {
			traces[traceIDs[i]] = time.Time{}
			continue
		}
		timestamp, err := redis.Int64(value, nil)
		if err != nil {
			return nil, err
		}
		traces[traceIDs[i]] = time.Unix(timestamp, 0)
	}

	return traces, nil
}

func (t *traceStateMap) exists(traceID string) bool {
	exist, err := redis.Bool(t.traces.getScores(t.mapKey(), traceID))
	if err != nil {
		return false
	}

	return exist
}

func (t *traceStateMap) remove(traceIDs ...string) error {
	return t.traces.remove(t.mapKey(), traceIDs...)
}

// in order to create a new sorted set, we need to call ZADD for each traceID
// that operation takes O(N) time,	where N is the number of traceIDs
// Instead, sorted set supports checking multiple members for existence in a
// single command. Since we only have 6 states, that operation is constant.
// However, if most of the requests are for less than 6 traceIDs, then it's
// better to use the ZADD command.

func (t *traceStateMap) move(traceID string, destination *traceStateMap) error {
	// store the timestamp as a part of the entry in the trace state set
	// make it into a sorted set
	// the timestamp should be a fixed length unix timestamp

	_, err := t.traces.client.do("MULTI")
	if err != nil {
		return err
	}
	err = t.traces.add(destination.mapKey(), float64(time.Now().Unix()), traceID)
	if err != nil {
		return err
	}
	err = t.traces.remove(t.mapKey(), traceID)
	if err != nil {
		return err
	}

	_, err = t.traces.client.do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

type redisKeyDeleter interface {
	del(keys ...string) error
}

type redisSortedSet struct {
	client *redisConn
	redisKeyDeleter
}

func (r *redisSortedSet) add(key string, score float64, member ...interface{}) error {
	_, err := r.client.do("ZADD", key, score, member)
	return err
}

func (r *redisSortedSet) getAllFields(key string) ([]string, error) {
	return redis.Strings(r.client.do("ZRANGE", key, 0, -1))
}

func (r *redisSortedSet) getScores(key string, members ...interface{}) ([]interface{}, error) {
	command := "ZSCORE"
	if len(members) > 1 {
		command = "ZMSCORE"
	}
	return redis.Values(r.client.do(command, key, members))

}

func (r *redisSortedSet) remove(key string, members ...string) error {
	_, err := r.client.do("ZREM", key, members)
	return err
}

type redisHasher interface {
	set(key string, field string, value interface{}) error // sets field in the hash stored at key to value.
	mset(key string, fields map[string]interface{}) error  // sets the specified fields to their respective values in the hash stored at key.
	getAll(key string) ([]interface{}, error)              // returns all fields and values of the hash stored at key.
	getAllStruct(key string, out interface{}) error        // returns all fields and values of the hash stored at key.
	getAllSlice(key string, out interface{}) error         // returns all fields and values of the hash stored at key.
	getAllFields(key string) ([]interface{}, error)        // returns all fields of the hash stored at key.

	redisKeyDeleter
}

type redisHash struct {
	client *redisConn
}

func newRedisHash(conn *redisConn) *redisHash {
	return &redisHash{
		client: conn,
	}
}

func (r *redisHash) set(key string, field string, value interface{}) error {
	_, err := r.client.do("HSET", key, field, value)
	return err
}

func (r *redisHash) mset(key string, fields map[string]interface{}) error {
	args := make([]interface{}, 0, len(fields)+1)
	args = append(args, key)
	for k, v := range fields {
		args = append(args, k)
		args = append(args, v)
	}
	_, err := r.client.do("HMSET", args...)
	return err
}

func (r *redisHash) getAll(key string) ([]interface{}, error) {
	return redis.Values(r.client.do("HGETALL", key))
}

func (r *redisHash) getAllStruct(key string, out interface{}) error {
	result, err := r.getAllFields(key)
	if err != nil {
		return err
	}
	err = redis.ScanStruct(result, out)
	if err != nil {
		return fmt.Errorf("failed to get values for key %s with error %s ", key, err)
	}
	return nil
}

func (r *redisHash) getAllSlice(key string, out interface{}) error {
	result, err := r.getAllFields(key)
	if err != nil {
		return err
	}

	err = redis.ScanSlice(result, out)
	if err != nil {
		return fmt.Errorf("failed to get values for key %s with error %s ", key, err)
	}
	return nil
}

func (r *redisHash) getAllFields(key string) ([]interface{}, error) {
	return redis.Values(r.client.do("HKEYS", key))
}

func (r *redisHash) del(keys ...string) error {
	_, err := r.client.do("DEL", keys)
	return err
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

func (r *redisConn) do(commandName string, args ...interface{}) (reply interface{}, err error) {
	conn := r.pool.Get()
	defer conn.Close()

	return conn.Do(commandName, args...)
}

// func Intersect(keys ...string) ([]interface{}, error) {
//		// upload the trace IDs into a temp set(with a TTL on each trace ID) in redis and then use the SINTER command to get the intersection of the sets
//			set := RedisSortedSet{client: r.client}
//	// TODO: set expiration time
//	key := fmt.Sprintf("%d:tmp", time.Now().Unix())
//	err = set.Add(key, 0, traceIDs)
//	if err != nil {
//		return nil, err
//	}
//	reply, err := set.client.Do("ZINTER",
//		key, len(r.statuses), r.statuses[Collecting].MapKey(), r.statuses[WaitingToDecide].MapKey(),
//		r.statuses[ReadyForDecision].MapKey(), r.statuses[AwaitingDecision].MapKey(), "AGGREGATE", "MAX", "WITHSCORES")
//	if err != nil {
//		return nil, err
//	}
//}
