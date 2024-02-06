package centralstore

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	internal "github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/metrics"
)

// TODO: shard the trace state map by timestamp so that we can set TTLs on the keys
// if the TTL is within a certain range, we should start a new shard
type RedisRemoteStoreOptions struct {
	Host  string
	Cache config.SampleCacheConfig
}

var _ BasicStorer = (*RedisBasicStore)(nil)

func NewRedisBasicStore(opt RedisRemoteStoreOptions) *RedisBasicStore {
	var host string
	if opt.Host == "" {
		host = "localhost:6379"
	}

	redisClient := internal.NewClient(&internal.Config{
		Addr: host,
	})

	decisionCache, err := cache.NewCuckooSentCache(opt.Cache, &metrics.NullMetrics{})
	if err != nil {
		panic(err)
	}
	return &RedisBasicStore{
		client:        redisClient,
		decisionCache: decisionCache,
		statuses:      newTraceStatusStore(redisClient),
		states: map[CentralTraceState]*traceStateMap{
			Collecting:       newTraceStateMap(redisClient, Collecting),
			WaitingToDecide:  newTraceStateMap(redisClient, WaitingToDecide),
			ReadyForDecision: newTraceStateMap(redisClient, ReadyForDecision),
			AwaitingDecision: newTraceStateMap(redisClient, AwaitingDecision),
		},
	}
}

// RedisBasicStore is an implementation of RemoteStore.
type RedisBasicStore struct {
	client        *internal.Client
	decisionCache cache.TraceSentCache
	statuses      *traceStatusStore
	states        map[CentralTraceState]*traceStateMap
}

func (r *RedisBasicStore) Close() error {
	return r.client.Stop(nil)
}

func (r *RedisBasicStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}

func (r *RedisBasicStore) findTraceStatus(traceID string) (CentralTraceState, *CentralTraceStatus) {
	if tracerec, _, found := r.decisionCache.Test(traceID); found {
		// it was in the decision cache, so we can return the right thing
		if tracerec.Kept() {
			return DecisionKeep, NewCentralTraceStatus(traceID, DecisionKeep)
		} else {
			return DecisionDrop, NewCentralTraceStatus(traceID, DecisionDrop)
		}
	}

	if r.states[WaitingToDecide].exists(traceID) {
		return WaitingToDecide, NewCentralTraceStatus(traceID, WaitingToDecide)
	}

	if r.states[Collecting].exists(traceID) {
		return Collecting, NewCentralTraceStatus(traceID, Collecting)
	}

	return Unknown, nil
}

func (r *RedisBasicStore) WriteSpan(span *CentralSpan) error {
	state, _ := r.findTraceStatus(span.TraceID)

	switch state {
	case DecisionDrop:
		return nil
	case DecisionKeep, AwaitingDecision:
		_ = r.statuses.incrementSpanCounts(nil, span.TraceID, string(span.Type))
	case Collecting:
		if span.ParentID == "" {
			err := r.states[Collecting].move(r.states[WaitingToDecide], span.TraceID)
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
		err = r.statuses.addStatus(span)
		if err != nil {
			return err
		}
	}

	err := r.statuses.addSpan(span)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) GetTrace(traceID string) (*CentralTrace, error) {
	return r.statuses.getTrace(traceID)
}

func (r *RedisBasicStore) GetStatusForTraces(traceIDs []string) ([]*CentralTraceStatus, error) {
	statusesMap, err := r.statuses.getTraceStatuses(traceIDs)
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
		state := Unknown
		if tracerec, _, found := r.decisionCache.Test(id); found {
			// it was in the decision cache, so we can return the right thing
			if tracerec.Kept() {
				state = DecisionKeep
			} else {
				state = DecisionDrop
			}
		}

		status, ok := statusesMap[id]
		if !ok {
			status = NewCentralTraceStatus(id, state)
			statusesMap[id] = status
			continue
		}

		status.State = state
	}

	statuses := make([]*CentralTraceStatus, 0, len(statusesMap))
	for _, status := range statusesMap {
		statuses = append(statuses, status)
	}

	return statuses, nil

}

func (r *RedisBasicStore) GetTracesForState(state CentralTraceState) ([]string, error) {
	switch state {
	case DecisionDrop, DecisionKeep:
		return nil, nil
	}
	return r.states[state].getAllTraces()
}

func (r *RedisBasicStore) ChangeTraceStatus(traceIDs []string, fromState, toState CentralTraceState) error {
	if toState == DecisionDrop {
		traces, err := r.statuses.getTraceStatuses(traceIDs)
		if err != nil {
			return err
		}

		for _, trace := range traces {
			r.decisionCache.Record(trace, false, "")
		}

		return r.removeTraces(traceIDs)
	}

	err := r.changeTraceStatus(traceIDs, fromState, toState)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) KeepTraces(statuses []*CentralTraceStatus) error {
	traceIDs := make([]string, 0, len(statuses))
	for _, status := range statuses {
		traceIDs = append(traceIDs, status.TraceID)
	}
	err := r.changeTraceStatus(traceIDs, AwaitingDecision, DecisionKeep)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) removeTraces(traceIDs []string) error {
	err := r.statuses.remove(traceIDs)
	if err != nil {
		return err
	}

	return r.states[AwaitingDecision].remove(traceIDs...)
}

func (r *RedisBasicStore) changeTraceStatus(traceIDs []string, fromState, toState CentralTraceState) error {
	err := r.states[fromState].move(r.states[toState], traceIDs...)
	if err != nil {
		return err
	}

	return nil
}

// TraceStore stores trace state status and spans.
// trace state statuses is stored in a redis hash with the key being the trace ID
// and each field being a status field.
// for example, an entry in the hash would be: "trace1:trace" -> "state:DecisionKeep, rate:100, reason:reason1"
// spans are stored in a redis hash with the key being the trace ID and each field being a span ID and the value being the serialized CentralSpan.
// for example, an entry in the hash would be: "trace1:spans" -> "span1:{spanID:span1, KeyFields: []}, span2:{spanID:span2, KeyFields: []}"
type traceStatusStore struct {
	spans *spansList
	redis *internal.Client
}

func newTraceStatusStore(client *internal.Client) *traceStatusStore {
	spans := NewSpanList(client)
	return &traceStatusStore{
		spans: spans,
		redis: client,
	}
}

func (t *traceStatusStore) addStatus(span *CentralSpan) error {
	conn := t.redis.Get()
	defer conn.Close()

	trace := &CentralTraceStatus{
		TraceID: span.TraceID,
	}

	err := conn.SetHash(t.traceStateKey(span.TraceID), trace)
	if err != nil {
		return err
	}

	return nil

}

func (t *traceStatusStore) getTrace(traceID string) (*CentralTrace, error) {
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

func (t *traceStatusStore) getTraceStatuses(traceIDs []string) (map[string]*CentralTraceStatus, error) {
	conn := t.redis.Get()
	defer conn.Close()

	statuses := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, traceID := range traceIDs {
		status := &CentralTraceStatus{}
		err := conn.GetStructHash(t.traceStateKey(traceID), status)
		if err != nil {
			return nil, err
		}

		statuses[traceID] = status
	}

	if len(statuses) == 0 {
		return nil, fmt.Errorf("no trace found")

	}

	return statuses, nil
}

func (t *traceStatusStore) getSpans(traceID string) ([]*CentralSpan, *CentralSpan, error) {
	return t.spans.getAll(traceID)
}

func (t *traceStatusStore) traceStateKey(traceID string) string {
	return fmt.Sprintf("%s:trace", traceID)
}

func (t *traceStatusStore) addSpan(span *CentralSpan) error {
	conn := t.redis.Get()
	defer conn.Close()

	err := t.spans.addSpan(span)
	if err != nil {
		return err
	}

	return t.incrementSpanCounts(conn, span.TraceID, string(span.Type))
}

func (t *traceStatusStore) incrementSpanCounts(conn internal.Conn, traceID string, spanType string) error {
	if conn == nil {
		conn = t.redis.Get()
		defer conn.Close()
	}

	var field string
	switch spanType {
	case "event":
		field = "span_event_count"
	case "link":
		field = "span_link_count"
	default:
		field = "span_count"

	}
	_, err := conn.IncrementByHash(t.traceStateKey(traceID), field, 1)
	return err
}

func (t *traceStatusStore) remove(traceIDs []string) error {
	conn := t.redis.Get()
	defer conn.Close()

	// remove span list

	err := t.spans.remove(traceIDs)
	if err != nil {
		return err
	}
	// remove trace status
	_, err = conn.Del(traceIDs...)
	return err
}

// spansList stores spans in a redis hash with the key being the trace ID and each field
// being a span ID and the value being the serialized CentralSpan.
type spansList struct {
	redis *internal.Client
}

func NewSpanList(redis *internal.Client) *spansList {
	return &spansList{
		redis: redis,
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
	conn := s.redis.Get()
	defer conn.Close()

	data, err := s.serialize(span)
	if err != nil {
		return err
	}

	// overwrite the span data if it already exists
	return conn.SetHash(s.spansKey(span.TraceID), map[string]any{s.spansField(span.SpanID, string(span.Type)): data})
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
	conn := s.redis.Get()
	defer conn.Close()

	spans := make([]*CentralSpan, 0)
	var tmpSpan []struct {
		SpanID string
		Span   []byte
	}

	err := conn.GetSliceOfStructsHash(s.spansKey(traceID), &tmpSpan)
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

func (s *spansList) remove(traceIDs []string) error {
	conn := s.redis.Get()
	defer conn.Close()

	_, err := conn.Del(traceIDs...)
	return err
}

// traceStateMap is a map of trace IDs to their state.
// It's used to atomically move traces between states and to get all traces in a state.
// In order to also record the time of the state change, we also store traceID:timestamp
// in a hash using <state>:traces:time as the hash key.
// By using a hash for the timestamps, we can easily get the timestamp for a trace based
// on it's current state.
type traceStateMap struct {
	redis *internal.Client
	state CentralTraceState
}

func newTraceStateMap(redis *internal.Client, state CentralTraceState) *traceStateMap {
	return &traceStateMap{
		redis: redis,
		state: state,
	}
}

func (t *traceStateMap) addTrace(traceID string) error {
	conn := t.redis.Get()
	defer conn.Close()

	return conn.ZAdd(t.mapKey(), []any{time.Now().Unix(), traceID})
}

func (t *traceStateMap) mapKey() string {
	return fmt.Sprintf("%s:traces", t.state)
}

func (t *traceStateMap) getAllTraces() ([]string, error) {
	conn := t.redis.Get()
	defer conn.Close()

	results, err := conn.ZRange(t.mapKey(), 0, -1)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (t *traceStateMap) getTraces(traceIDs []string) (map[string]time.Time, error) {
	conn := t.redis.Get()
	defer conn.Close()

	timestamps, err := conn.ZMScore(t.mapKey(), traceIDs)
	if err != nil {
		return nil, err
	}

	traces := make(map[string]time.Time, len(timestamps))
	for i, value := range timestamps {
		if value == 0 {
			traces[traceIDs[i]] = time.Time{}
			continue
		}
		traces[traceIDs[i]] = time.Unix(value, 0)
	}

	return traces, nil
}

func (t *traceStateMap) exists(traceID string) bool {
	conn := t.redis.Get()
	defer conn.Close()

	exist, err := redis.Bool(conn.ZScore(t.mapKey(), traceID))
	if err != nil {
		return false
	}

	return exist
}

func (t *traceStateMap) remove(traceIDs ...string) error {
	conn := t.redis.Get()
	defer conn.Close()

	return conn.ZRemove(t.mapKey(), traceIDs)
}

// in order to create a new sorted set, we need to call ZADD for each traceID
// that operation takes O(N) time,	where N is the number of traceIDs
// Instead, sorted set supports checking multiple members for existence in a
// single command. Since we only have 6 states, that operation is constant.
// However, if most of the requests are for less than 6 traceIDs, then it's
// better to use the ZADD command.

func (t *traceStateMap) move(destination *traceStateMap, traceIDs ...string) error {
	// store the timestamp as a part of the entry in the trace state set
	// make it into a sorted set
	// the timestamp should be a fixed length unix timestamp
	conn := t.redis.Get()
	defer conn.Close()

	// only add traceIDs to the destination if they don't already exist
	entries := make([]any, len(traceIDs)*2)
	for i := range entries {
		if i%2 == 0 {
			entries[i] = time.Now().Unix()
			continue
		}
		entries[i] = traceIDs[i/2]
	}
	err := conn.ZAdd(destination.mapKey(), entries)
	if err != nil && err != redis.ErrNil {
		return err
	}

	_ = conn.ZRemove(t.mapKey(), traceIDs)

	return nil
}
