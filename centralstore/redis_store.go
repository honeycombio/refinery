package centralstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
)

const (
	expirationForTraceStatus = 24 * time.Hour
	expirationForTraceState  = 24 * time.Hour
	// DefaultPendingWorkCapacity how many events to queue up for busy batches
	defaultPendingWorkCapacity = 10000
)

var validStateChangeEvents = map[string]struct{}{
	"unknown-collecting":                   {},
	"collecting-waiting_to_decide":         {},
	"waiting_to_decide-ready_for_decision": {},
	"ready_for_decision-awaiting_decision": {},
	"awaiting_decision-ready_for_decision": {},
}

type RedisBasicStoreOptions struct {
	Host              string
	Cache             config.SampleCacheConfig
	MaxTraceRetention time.Duration
}

var _ BasicStorer = (*RedisBasicStore)(nil)

func NewRedisBasicStore(opt RedisBasicStoreOptions) *RedisBasicStore {
	var host string
	if opt.Host == "" {
		host = "localhost:6379"
	}

	if opt.MaxTraceRetention == 0 {
		opt.MaxTraceRetention = 24 * time.Hour
	}

	redisClient := redis.NewClient(&redis.Config{
		Addr: host,
	})

	if err := ensureValidStateChangeEvents(redisClient); err != nil {
		panic(err)
	}

	decisionCache, err := cache.NewCuckooSentCache(opt.Cache, &metrics.NullMetrics{})
	if err != nil {
		panic(err)
	}

	stateProcessorCfg := traceStateProcessorConfig{
		reaperRunInterval: 10 * time.Second,
		maxTraceRetention: opt.MaxTraceRetention,
		changeState:       redisClient.NewScript(stateChangeKey, stateChangeScript),
	}

	clock := clockwork.NewRealClock()
	stateProcessor := newTraceStateProcessor(stateProcessorCfg, clock)

	ctx := context.Background()
	childCtx, cancle := context.WithCancel(ctx)
	go stateProcessor.cleanupExpiredTraces(childCtx, redisClient)

	return &RedisBasicStore{
		cancel:        cancle,
		client:        redisClient,
		decisionCache: decisionCache,
		traces:        newTraceStatusStore(clock),
		states:        stateProcessor,
		errs:          make(chan error, defaultPendingWorkCapacity),
	}
}

// RedisBasicStore is an implementation of BasicStorer that uses Redis as the backing store.
type RedisBasicStore struct {
	cancel        context.CancelFunc
	client        redis.Client
	decisionCache cache.TraceSentCache
	traces        *tracesStore
	states        *testStateProcessor
	errs          chan error
}

func (r *RedisBasicStore) Close(ctx context.Context) error {
	r.cancel()
	r.states.Stop()
	if err := r.client.Stop(ctx); err != nil {
		r.errs <- err
	}
	close(r.errs)
	return nil
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
			err := r.states.toNextState(conn, newTraceStateChangeEvent(Collecting, WaitingToDecide), span.TraceID)
			if err != nil {
				r.errs <- err
			}
		}
	case WaitingToDecide, ReadyForDecision:
	case Unknown:
		err := r.states.addNewTrace(conn, span.TraceID)
		if err != nil {
			r.errs <- err
		}
		err = r.traces.addStatus(conn, span)
		if err != nil {
			r.errs <- err
		}
	}

	err := r.traces.storeSpan(conn, span)
	if err != nil {
		r.errs <- err
	}

	return nil
}

func (r *RedisBasicStore) GetTrace(traceID string) (*CentralTrace, error) {
	// TODO: what if this trace is already in the decision cache?
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
	for _, state := range r.states.states {
		if len(remaining) == 0 {
			break
		}

		exist, err := r.states.traceIDsWithTimestamp(conn, state, remaining)
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

	return r.states.allTraceIDs(conn, state)
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

	if r.states.exists(conn, AwaitingDecision, traceID) {
		return AwaitingDecision, NewCentralTraceStatus(traceID, AwaitingDecision)
	}

	if r.states.exists(conn, ReadyForDecision, traceID) {
		return ReadyForDecision, NewCentralTraceStatus(traceID, ReadyForDecision)
	}

	if r.states.exists(conn, WaitingToDecide, traceID) {
		return WaitingToDecide, NewCentralTraceStatus(traceID, WaitingToDecide)
	}

	if r.states.exists(conn, Collecting, traceID) {
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

	return r.states.remove(conn, AwaitingDecision, traceIDs...)
}

func (r *RedisBasicStore) changeTraceStatus(conn redis.Conn, traceIDs []string, fromState, toState CentralTraceState) error {
	err := r.states.toNextState(conn, newTraceStateChangeEvent(fromState, toState), traceIDs...)
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
type tracesStore struct {
	clock clockwork.Clock
}

func newTraceStatusStore(clock clockwork.Clock) *tracesStore {
	return &tracesStore{
		clock: clock,
	}
}

func (t *tracesStore) addStatus(conn redis.Conn, span *CentralSpan) error {
	trace := &CentralTraceStatus{
		TraceID:   span.TraceID,
		timestamp: t.clock.Now().UTC(),
	}

	_, err := conn.SetHashTTL(t.traceStatusKey(span.TraceID), trace, expirationForTraceStatus)
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

// TraceStateProcessor is a map of trace IDs to their state.
// It's used to atomically move traces between states and to get all traces in a state.
// In order to also record the time of the state change, we also store traceID:timestamp
// in a hash using <state>:traces:time as the hash key.
// By using a hash for the timestamps, we can easily get the timestamp for a trace based
// on it's current state.

type traceStateProcessorConfig struct {
	reaperRunInterval time.Duration
	maxTraceRetention time.Duration
	changeState       redis.Script
}

type testStateProcessor struct {
	states       []CentralTraceState
	config       traceStateProcessorConfig
	clock        clockwork.Clock
	reaperTicker clockwork.Ticker
}

func newTraceStateProcessor(cfg traceStateProcessorConfig, clock clockwork.Clock) *testStateProcessor {
	if cfg.reaperRunInterval == 0 {
		cfg.reaperRunInterval = 10 * time.Second
	}
	if cfg.maxTraceRetention == 0 {
		cfg.maxTraceRetention = 24 * time.Hour
	}
	s := &testStateProcessor{
		states: []CentralTraceState{
			Collecting,
			WaitingToDecide,
			ReadyForDecision,
			AwaitingDecision,
		},
		config:       cfg,
		clock:        clock,
		reaperTicker: clock.NewTicker(cfg.reaperRunInterval),
	}

	return s
}

func (t *testStateProcessor) Stop() {
	if t.reaperTicker != nil {
		t.reaperTicker.Stop()
	}
}

// addTrace stores the traceID into a set and insert the current time into
// a list. The list is used to keep track of the time the trace was added to
// the state. The set is used to check if the trace is in the state.
func (t *testStateProcessor) addNewTrace(conn redis.Conn, traceID string) error {
	if !t.isValidStateChange(conn, newTraceStateChangeEvent(Unknown, Collecting), traceID) {
		return nil
	}

	return conn.ZAdd(t.stateByTraceIDsKey(Collecting), []any{t.clock.Now().Unix(), traceID})
}

func (t *testStateProcessor) stateByTraceIDsKey(state CentralTraceState) string {
	return fmt.Sprintf("%s:traces", state)
}

func (t *testStateProcessor) traceStatesKey(traceID string) string {
	return fmt.Sprintf("%s:states", traceID)
}

func (t *testStateProcessor) allTraceIDs(conn redis.Conn, state CentralTraceState) ([]string, error) {
	results, err := conn.ZRange(t.stateByTraceIDsKey(state), 0, -1)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (t *testStateProcessor) traceIDsWithTimestamp(conn redis.Conn, state CentralTraceState, traceIDs []string) (map[string]time.Time, error) {
	timestamps, err := conn.ZMScore(t.stateByTraceIDsKey(state), traceIDs)
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

func (t *testStateProcessor) exists(conn redis.Conn, state CentralTraceState, traceID string) bool {
	exist, err := conn.ZExist(t.stateByTraceIDsKey(state), traceID)
	if err != nil {
		return false
	}

	return exist
}

func (t *testStateProcessor) remove(conn redis.Conn, state CentralTraceState, traceIDs ...string) error {
	return conn.ZRemove(t.stateByTraceIDsKey(state), traceIDs)
}

func (t *testStateProcessor) toNextState(conn redis.Conn, changeEvent stateChangeEvent, traceIDs ...string) error {
	eligible := make([]any, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		if t.isValidStateChangeThroughLua(conn, changeEvent, traceID) {
			eligible = append(eligible, traceID)
		}
	}

	if len(eligible) == 0 {
		return errors.New("invalid state change event")
	}

	// store the timestamp as a part of the entry in the trace state set
	// make it into a sorted set
	// the timestamp should be a fixed length unix timestamp
	timestamps := make([]int64, len(eligible))
	for i := range eligible {
		timestamps[i] = t.clock.Now().Unix()
	}

	// only add traceIDs to the destination if they don't already exist
	return conn.ZMove(t.stateByTraceIDsKey(changeEvent.current), t.stateByTraceIDsKey(changeEvent.next), timestamps, eligible)
}

func (t *testStateProcessor) isValidStateChange(conn redis.Conn, stateChange stateChangeEvent, traceID string) bool {
	// We don't need to call unwatch here because `RPushTTL` is executed in a transaction
	// Redis will automatically discard the watch after the transaction is executed
	current := stateChange.current
	_, err := conn.Watch(t.traceStatesKey(traceID), func(c redis.Conn) error {
		currentStateRecorded, err := conn.LIndexString(t.traceStatesKey(traceID), -1)
		if err != nil {
			return err
		}

		if currentStateRecorded != "" {
			current = CentralTraceState(currentStateRecorded)
		}

		stateChangeEvent := newTraceStateChangeEvent(current, stateChange.next)
		if _, ok := validStateChangeEvents[stateChangeEvent.string()]; !ok {
			return fmt.Errorf("invalid state change event")
		}
		if ok, err := conn.RPushTTL(t.traceStatesKey(traceID), stateChange.next.String(), expirationForTraceState); err != nil || !ok {
			return err
		}

		return nil
	})
	return err == nil
}

// cleanupExpiredTraces removes traces from the state map if they have been in the state for longer than
// the configured time.
func (t *testStateProcessor) cleanupExpiredTraces(ctx context.Context, redis redis.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.reaperTicker.Chan():
			t.removeExpiredTraces(redis)
		}
	}
}

func (t *testStateProcessor) removeExpiredTraces(client redis.Client) {
	conn := client.Get()
	defer conn.Close()

	// get the traceIDs that have been in the state for longer than the expiration time
	for _, state := range t.states {
		traceIDs, err := conn.ZRangeByScoreString(t.stateByTraceIDsKey(state), t.clock.Now().Add(-t.config.maxTraceRetention).Unix())
		if err != nil {
			return
		}

		if len(traceIDs) == 0 {
			continue
		}

		// remove the traceIDs from the state map
		err = t.remove(conn, state, traceIDs...)
		if err != nil {
			continue
		}
	}

}

// get the latest state from the state list
// check if the current state change event is valid
// if it is, then add the state to state list
// and add the trace to the state set
// if it's not, then don't add the state to the state list	and abort
func (t *testStateProcessor) isValidStateChangeThroughLua(conn redis.Conn, stateChange stateChangeEvent, traceID string) bool {
	result, err := t.config.changeState.Do(context.TODO(), conn, t.traceStatesKey(traceID), validStateChangeEventsKey, stateChange.current.String(), stateChange.next.String(), expirationForTraceState.Seconds())
	if err != nil {
		return false
	}
	val, ok := result.(int64)
	if !ok {
		return false
	}

	return val == 1
}

const stateChangeKey = 2
const stateChangeScript = `
  local traceStateKey = KEYS[1]
  local possibleStateChangeEvents = KEYS[2]
  local previousState = ARGV[1]
  local nextState = ARGV[2]
  local ttl = ARGV[3]

  local currentState = redis.call('LINDEX', traceStateKey, -1)
  if (currentState == nil or currentState == false) then
	currentState = previousState
  end

  local stateChangeEvent = string.format("%s-%s", currentState, nextState)
  local changeEventIsValid = redis.call('SISMEMBER', possibleStateChangeEvents, stateChangeEvent)
  if (changeEventIsValid == 0) then 
    do return -1 end
  end


  redis.call('RPUSH', traceStateKey, nextState)
  redis.call('EXPIRE', traceStateKey, ttl)
  return 1
`

const validStateChangeEventsKey = "valid-state-change-events"

func ensureValidStateChangeEvents(client redis.Client) error {
	conn := client.Get()
	defer conn.Close()

	return conn.SAdd(validStateChangeEventsKey,
		newTraceStateChangeEvent(Unknown, Collecting).string(),
		newTraceStateChangeEvent(Collecting, WaitingToDecide).string(),
		newTraceStateChangeEvent(WaitingToDecide, ReadyForDecision).string(),
		newTraceStateChangeEvent(ReadyForDecision, AwaitingDecision).string(),
		newTraceStateChangeEvent(AwaitingDecision, ReadyForDecision).string(),
	)
}

type stateChangeEvent struct {
	current CentralTraceState
	next    CentralTraceState
}

func newTraceStateChangeEvent(current, next CentralTraceState) stateChangeEvent {
	return stateChangeEvent{
		current: current,
		next:    next,
	}
}

func (s stateChangeEvent) string() string {
	return fmt.Sprintf("%s-%s", s.current, s.next)
}

// isValidStateChange
//BenchmarkStoreGetTrace-12    	    3793	    340487 ns/op	   15467 B/op	     108 allocs/op
//PASS
//ok  	github.com/honeycombio/refinery/centralstore	3.520s

// isValidStateChangeThroughLua
//BenchmarkStoreGetTrace-12    	    3916	    339635 ns/op	   15398 B/op	     107 allocs/op
//PASS
//ok  	github.com/honeycombio/refinery/centralstore	3.564s
