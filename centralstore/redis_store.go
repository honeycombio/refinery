package centralstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/metrics"
)

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

	redisClient := redis.NewClient(&redis.Config{
		Addr: host,
	})

	decisionCache, err := cache.NewCuckooSentCache(opt.Cache, &metrics.NullMetrics{})
	if err != nil {
		panic(err)
	}

	return &RedisBasicStore{
		client:        redisClient,
		decisionCache: decisionCache,
		traces:        newTraceStatusStore(),
		states: map[CentralTraceState]*traceStateMap{
			Collecting:       newTraceStateMap(Collecting),
			WaitingToDecide:  newTraceStateMap(WaitingToDecide),
			ReadyForDecision: newTraceStateMap(ReadyForDecision),
			AwaitingDecision: newTraceStateMap(AwaitingDecision),
		},
	}
}

// RedisBasicStore is an implementation of BasicStorer that uses Redis as the backing store.
type RedisBasicStore struct {
	client        redis.Client
	decisionCache cache.TraceSentCache
	traces        *tracesStore
	states        map[CentralTraceState]*traceStateMap
}

func (r *RedisBasicStore) Close() error {
	return r.client.Stop(context.TODO())
}

func (r *RedisBasicStore) GetMetrics() (map[string]interface{}, error) {
	return nil, nil
}

func (r *RedisBasicStore) WriteSpan(span *CentralSpan) error {
	conn := r.client.Get()
	defer conn.Close()

	state, _ := r.findTraceStatus(conn, span.TraceID)

	switch state {
	case DecisionDrop:
		return nil
	case DecisionKeep, AwaitingDecision:
		_ = r.traces.incrementSpanCounts(conn, span.TraceID, span.Type)
	case Collecting:
		if span.ParentID == "" {
			err := r.states[Collecting].move(conn, r.states[WaitingToDecide], span.TraceID)
			if err != nil {
				return err
			}
		}
	case WaitingToDecide, ReadyForDecision:
	case Unknown:
		err := r.states[Collecting].addTrace(conn, span.TraceID)
		if err != nil {
			return err
		}
		err = r.traces.addStatus(conn, span)
		if err != nil {
			return err
		}
	}

	err := r.traces.storeSpan(conn, span)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) GetTrace(traceID string) (*CentralTrace, error) {
	conn := r.client.Get()
	defer conn.Close()

	return r.traces.getTrace(conn, traceID)
}

func (r *RedisBasicStore) GetStatusForTraces(traceIDs []string) ([]*CentralTraceStatus, error) {
	conn := r.client.Get()
	defer conn.Close()

	statusesMap, err := r.traces.getTraceStatuses(conn, traceIDs)
	if err != nil {
		return nil, err
	}

	remaining := traceIDs
	for state, status := range r.states {
		if len(remaining) == 0 {
			break
		}

		exist, err := status.getTraces(conn, remaining)
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

	conn := r.client.Get()
	defer conn.Close()

	return r.states[state].getAllTraces(conn)
}

func (r *RedisBasicStore) ChangeTraceStatus(traceIDs []string, fromState, toState CentralTraceState) error {
	conn := r.client.Get()
	defer conn.Close()

	if toState == DecisionDrop {
		traces, err := r.traces.getTraceStatuses(conn, traceIDs)
		if err != nil {
			return err
		}

		for _, trace := range traces {
			r.decisionCache.Record(trace, false, "")
		}

		return r.removeTraces(traceIDs)
	}

	err := r.changeTraceStatus(conn, traceIDs, fromState, toState)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) KeepTraces(statuses []*CentralTraceStatus) error {
	conn := r.client.Get()
	defer conn.Close()

	traceIDs := make([]string, 0, len(statuses))
	for _, status := range statuses {
		traceIDs = append(traceIDs, status.TraceID)
	}
	err := r.changeTraceStatus(conn, traceIDs, AwaitingDecision, DecisionKeep)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) findTraceStatus(conn redis.Conn, traceID string) (CentralTraceState, *CentralTraceStatus) {
	if tracerec, _, found := r.decisionCache.Test(traceID); found {
		// it was in the decision cache, so we can return the right thing
		if tracerec.Kept() {
			return DecisionKeep, NewCentralTraceStatus(traceID, DecisionKeep)
		} else {
			return DecisionDrop, NewCentralTraceStatus(traceID, DecisionDrop)
		}
	}

	if r.states[WaitingToDecide].exists(conn, traceID) {
		return WaitingToDecide, NewCentralTraceStatus(traceID, WaitingToDecide)
	}

	if r.states[Collecting].exists(conn, traceID) {
		return Collecting, NewCentralTraceStatus(traceID, Collecting)
	}

	return Unknown, nil
}

func (r *RedisBasicStore) removeTraces(traceIDs []string) error {
	conn := r.client.Get()
	defer conn.Close()

	err := r.traces.remove(conn, traceIDs)
	if err != nil {
		return err
	}

	return r.states[AwaitingDecision].remove(conn, traceIDs...)
}

func (r *RedisBasicStore) changeTraceStatus(conn redis.Conn, traceIDs []string, fromState, toState CentralTraceState) error {
	err := r.states[fromState].move(conn, r.states[toState], traceIDs...)
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
type tracesStore struct{}

func newTraceStatusStore() *tracesStore {
	return &tracesStore{}
}

func (t *tracesStore) addStatus(conn redis.Conn, span *CentralSpan) error {
	trace := &CentralTraceStatus{
		TraceID: span.TraceID,
	}

	err := conn.SetHash(t.traceStatusKey(span.TraceID), trace)
	if err != nil {
		return err
	}

	return nil

}

func (t *tracesStore) getTrace(conn redis.Conn, traceID string) (*CentralTrace, error) {
	spans, rootSpan, err := t.getSpansByTraceID(conn, traceID)
	if err != nil {
		return nil, err
	}

	return &CentralTrace{
		TraceID: traceID,
		Root:    rootSpan,
		Spans:   spans,
	}, nil
}

func (t *tracesStore) getTraceStatuses(conn redis.Conn, traceIDs []string) (map[string]*CentralTraceStatus, error) {
	statuses := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, traceID := range traceIDs {
		status := &CentralTraceStatus{}
		err := conn.GetStructHash(t.traceStatusKey(traceID), status)
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

func (t *tracesStore) getSpansByTraceID(conn redis.Conn, traceID string) ([]*CentralSpan, *CentralSpan, error) {
	return getAllSpans(conn, traceID)
}

func (t *tracesStore) traceStatusKey(traceID string) string {
	return fmt.Sprintf("%s:status", traceID)
}

// storeSpan stores the span in the spans hash and increments the span count for the trace.
func (t *tracesStore) storeSpan(conn redis.Conn, span *CentralSpan) error {
	var err error
	commands := make([]redis.Command, 2)
	commands[0], err = addToSpanHash(span)
	if err != nil {
		return err
	}

	commands[1], err = t.incrementSpanCountsCMD(span.TraceID, span.Type)
	if err != nil {
		return err
	}

	return conn.Exec(commands...)
}

func (t *tracesStore) incrementSpanCounts(conn redis.Conn, traceID string, spanType SpanType) error {
	cmd, err := t.incrementSpanCountsCMD(traceID, spanType)
	if err != nil {
		return err
	}

	_, err = conn.Do(cmd.Name(), cmd.Args()...)
	return err
}

func (t *tracesStore) incrementSpanCountsCMD(traceID string, spanType SpanType) (redis.Command, error) {

	var field string
	switch spanType {
	case SpanTypeEvent:
		field = "span_count_event"
	case SpanTypeLink:
		field = "span_count_link"
	default:
		field = "span_count"
	}

	return redis.NewIncrByHashCommand(t.traceStatusKey(traceID), field, 1), nil
}

func (t *tracesStore) remove(conn redis.Conn, traceIDs []string) error {
	keys := make([]string, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		keys = append(keys, t.traceStatusKey(traceID), spansHashByTraceIDKey(traceID))
	}

	_, err := conn.Del(keys...)
	return err
}

func spansHashByTraceIDKey(traceID string) string {
	return fmt.Sprintf("%s:spans", traceID)
}

// central span -> blobs
func addToSpanHash(span *CentralSpan) (redis.Command, error) {
	data, err := json.Marshal(span)
	if err != nil {
		return nil, err
	}

	// overwrite the span data if it already exists
	return redis.NewSetHashCommand(spansHashByTraceIDKey(span.TraceID), map[string]any{span.SpanID: data}), nil
}

func getAllSpans(conn redis.Conn, traceID string) ([]*CentralSpan, *CentralSpan, error) {
	spans := make([]*CentralSpan, 0)
	var tmpSpan []struct {
		SpanID string
		Span   []byte
	}

	err := conn.GetSliceOfStructsHash(spansHashByTraceIDKey(traceID), &tmpSpan)
	if err != nil {
		return spans, nil, err
	}

	var rootSpan *CentralSpan
	for _, d := range tmpSpan {
		span := &CentralSpan{}
		err := json.Unmarshal(d.Span, span)
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

// traceStateMap is a map of trace IDs to their state.
// It's used to atomically move traces between states and to get all traces in a state.
// In order to also record the time of the state change, we also store traceID:timestamp
// in a hash using <state>:traces:time as the hash key.
// By using a hash for the timestamps, we can easily get the timestamp for a trace based
// on it's current state.
type traceStateMap struct {
	state CentralTraceState
}

func newTraceStateMap(state CentralTraceState) *traceStateMap {
	s := &traceStateMap{
		state: state,
	}

	go s.cleanupExpiredTraces()

	return s
}

func (t *traceStateMap) addTrace(conn redis.Conn, traceID string) error {
	return conn.ZAdd(t.mapKey(), []any{time.Now().Unix(), traceID})
}

func (t *traceStateMap) mapKey() string {
	return fmt.Sprintf("%s:traces", t.state)
}

func (t *traceStateMap) getAllTraces(conn redis.Conn) ([]string, error) {
	results, err := conn.ZRange(t.mapKey(), 0, -1)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (t *traceStateMap) getTraces(conn redis.Conn, traceIDs []string) (map[string]time.Time, error) {
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

func (t *traceStateMap) exists(conn redis.Conn, traceID string) bool {
	exist, err := conn.ZExist(t.mapKey(), traceID)
	if err != nil {
		return false
	}

	return exist
}

func (t *traceStateMap) remove(conn redis.Conn, traceIDs ...string) error {
	return conn.ZRemove(t.mapKey(), traceIDs)
}

// in order to create a new sorted set, we need to call ZADD for each traceID
// that operation takes O(N) time,	where N is the number of traceIDs
// Instead, sorted set supports checking multiple members for existence in a
// single command. Since we only have 6 states, that operation is constant.
// However, if most of the requests are for less than 6 traceIDs, then it's
// better to use the ZADD command.

func (t *traceStateMap) move(conn redis.Conn, destination *traceStateMap, traceIDs ...string) error {
	// store the timestamp as a part of the entry in the trace state set
	// make it into a sorted set
	// the timestamp should be a fixed length unix timestamp
	entries := make([]any, len(traceIDs)*2)
	for i := range entries {
		if i%2 == 0 {
			entries[i] = time.Now().Unix()
			continue
		}
		entries[i] = traceIDs[i/2]
	}

	// only add traceIDs to the destination if they don't already exist
	return conn.ZMove(t.mapKey(), destination.mapKey(), entries)
}

// cleanupExpiredTraces removes traces from the state map if they have been in the state for longer than
// the configured time.
func (t *traceStateMap) cleanupExpiredTraces() {
}
