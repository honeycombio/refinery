package centralstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	expirationForTraceStatus   = 24 * time.Hour
	expirationForTraceState    = 24 * time.Hour
	defaultPendingWorkCapacity = 10000
	redigoTimestamp            = "2006-01-02 15:04:05.999999 -0700 MST"
)

type RedisBasicStoreOptions struct {
	Host              string
	Cache             config.SampleCacheConfig
	MaxTraceRetention time.Duration
}

func EnsureRedisBasicStoreOptions(opt *RedisBasicStoreOptions) {
	if opt == nil {
		opt = &RedisBasicStoreOptions{}
	}
	if opt.Host == "" {
		opt.Host = "localhost:6379"
	}

	if opt.MaxTraceRetention == 0 {
		opt.MaxTraceRetention = 24 * time.Hour
	}

	if opt.Cache.DroppedSize == 0 {
		opt.Cache.DroppedSize = 10000
	}

	if opt.Cache.KeptSize == 0 {
		opt.Cache.KeptSize = 100
	}

	if opt.Cache.SizeCheckInterval == 0 {
		opt.Cache.SizeCheckInterval = config.Duration(10 * time.Second)
	}

}

var _ BasicStorer = (*RedisBasicStore)(nil)

func NewRedisBasicStore(opt *RedisBasicStoreOptions) *RedisBasicStore {
	EnsureRedisBasicStoreOptions(opt)

	redisClient := redis.NewClient(&redis.Config{
		Addr:           opt.Host,
		MaxIdle:        10,
		MaxActive:      50,
		ConnectTimeout: 500 * time.Second,
		ReadTimeout:    500 * time.Second,
	})

	// TODO: a redis version check and return an error if the version is supported

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
	err = stateProcessor.Start(ctx, redisClient)
	if err != nil {
		panic(err)
	}

	return &RedisBasicStore{
		client:        redisClient,
		decisionCache: decisionCache,
		traces:        newTraceStatusStore(clock),
		states:        stateProcessor,
	}
}

// RedisBasicStore is an implementation of BasicStorer that uses Redis as the backing store.
type RedisBasicStore struct {
	client        redis.Client
	decisionCache cache.TraceSentCache
	traces        *tracesStore
	states        *traceStateProcessor
}

func (r *RedisBasicStore) Stop() error {
	r.states.Stop()
	if err := r.client.Stop(context.TODO()); err != nil {
		return err
	}
	return nil
}

func (r *RedisBasicStore) GetMetrics(ctx context.Context) (map[string]interface{}, error) {
	return nil, nil
}

func (r *RedisBasicStore) WriteSpan(ctx context.Context, span *CentralSpan) error {
	writeSpan := trace.SpanFromContext(ctx)

	conn := r.client.Get()
	defer conn.Close()

	state := r.getTraceState(ctx, conn, span.TraceID)

	otelutil.AddSpanFields(writeSpan, map[string]interface{}{
		"trace_id":  span.TraceID,
		"span_id":   span.SpanID,
		"parent_id": span.ParentID,
		"span_type": span.Type,
		"state":     state,
	})

	switch state {
	case DecisionDrop:
		return nil
	case DecisionKeep, AwaitingDecision:
		err := r.traces.incrementSpanCounts(ctx, conn, span.TraceID, span.Type)
		if err != nil {
			return err
		}
	case Collecting:
		if span.ParentID == "" {
			_, err := r.states.toNextState(ctx, conn, newTraceStateChangeEvent(Collecting, DecisionDelay), span.TraceID)
			if err != nil {
				return err
			}
		}
	case DecisionDelay, ReadyToDecide:
	case Unknown:
		err := r.states.addNewTrace(ctx, conn, span.TraceID)
		if err != nil {
			return err
		}
		err = r.traces.addStatus(ctx, conn, span)
		if err != nil {
			return err
		}
	}

	err := r.traces.storeSpan(ctx, conn, span)
	if err != nil {
		return err
	}

	return nil
}

// GetTrace returns a CentralTrace with the given traceID.
// if a decision has been made about the trace, the returned value
// will not contain span data.
func (r *RedisBasicStore) GetTrace(ctx context.Context, traceID string) (*CentralTrace, error) {
	ctx, span := r.tracer.Start(ctx, "GetConn")
	conn := r.client.Get()
	span.End()
	defer conn.Close()

	spans := make([]*CentralSpan, 0)
	var tmpSpan []struct {
		SpanID string
		Span   []byte
	}

	_, span = r.tracer.Start(ctx, "redisGetSpans")
	err := conn.GetSliceOfStructsHash(spansHashByTraceIDKey(traceID), &tmpSpan)
	if err != nil {
		return nil, err
	}
	span.End()

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

	return &CentralTrace{
		TraceID: traceID,
		Root:    rootSpan,
		Spans:   spans,
	}, nil
}

func (r *RedisBasicStore) GetStatusForTraces(ctx context.Context, traceIDs []string) ([]*CentralTraceStatus, error) {
	ctx, span := r.tracer.Start(ctx, "GetStatusForTraces")
	defer span.End()

	conn := r.client.Get()
	defer conn.Close()

	pendingTraceIDs := make([]string, 0, len(traceIDs))
	statusMap := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, id := range traceIDs {
		tracerec, reason, found := r.decisionCache.Test(id)
		if !found {
			pendingTraceIDs = append(pendingTraceIDs, id)
			continue
		}
		// it was in the decision cache, so we can return the right thing
		var state CentralTraceState
		if tracerec.Kept() {
			state = DecisionKeep
		} else {
			state = DecisionDrop
		}
		statusMap[id] = &CentralTraceStatus{
			TraceID:    id,
			State:      state,
			KeepReason: reason,
			Rate:       tracerec.Rate(),
			Count:      uint32(tracerec.SpanCount()),
			EventCount: uint32(tracerec.SpanEventCount()),
			LinkCount:  uint32(tracerec.SpanLinkCount()),
		}
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"num_of_pending_traces": len(pendingTraceIDs),
		"num_of_cached_traces":  len(statusMap),
	})

	statusMapFromRedis, err := r.traces.getTraceStatuses(ctx, conn, pendingTraceIDs)
	if err != nil {
		return nil, err
	}

	for _, state := range r.states.states {
		if len(pendingTraceIDs) == 0 {
			break
		}

		timestampsByTraceIDs, err := r.states.traceIDsWithTimestamp(ctx, conn, state, pendingTraceIDs)
		if err != nil {
			return nil, err
		}
		notExist := make([]string, 0, len(pendingTraceIDs))
		for id, timestamp := range timestampsByTraceIDs {
			if timestamp.IsZero() {
				notExist = append(notExist, id)
				continue
			}
			status, ok := statusMapFromRedis[id]
			if !ok {
				continue
			}
			status.State = state
			status.Timestamp = timestamp
			statusMap[id] = status
		}

		pendingTraceIDs = notExist
	}

	otelutil.AddSpanField(span, "num_of_unknown_traces", len(pendingTraceIDs))

	for _, id := range pendingTraceIDs {
		statusMap[id] = &CentralTraceStatus{
			TraceID: id,
			State:   Unknown,
		}
	}

	statuses := make([]*CentralTraceStatus, 0, len(statusMap))
	for _, status := range statusMap {
		statuses = append(statuses, status)
	}
	sort.SliceStable(statuses, func(i, j int) bool {
		if statuses[i].Timestamp.IsZero() {
			return false
		}
		return statuses[i].Timestamp.Before(statuses[j].Timestamp)
	})

	return statuses, nil

}

func (r *RedisBasicStore) GetTracesForState(ctx context.Context, state CentralTraceState) ([]string, error) {
	ctx, span := r.tracer.Start(ctx, "GetTracesForState")
	defer span.End()

	switch state {
	case DecisionDrop, DecisionKeep:
		otelutil.AddSpanField(span, "decision_made", true)
		return nil, nil
	}

	conn := r.client.Get()
	defer conn.Close()

	return r.states.allTraceIDs(ctx, conn, state, -1)
}

// GetTracesNeedingDecision returns a list of up to n trace IDs that are in the
// ReadyForDecision state. These IDs are moved to the AwaitingDecision state
// atomically, so that no other refinery will be assigned the same trace.
func (r *RedisBasicStore) GetTracesNeedingDecision(ctx context.Context, n int) ([]string, error) {
	ctx, span := r.tracer.Start(ctx, "GetTracesNeedingDecision")
	defer span.End()
	otelutil.AddSpanField(span, "batch_size", n)

	conn := r.client.Get()
	defer conn.Close()

	traceIDs, err := r.states.allTraceIDs(ctx, conn, ReadyToDecide, n)
	if err != nil {
		return nil, err
	}

	if len(traceIDs) == 0 {
		return nil, nil
	}

	succeed, err := r.states.toNextState(ctx, conn, newTraceStateChangeEvent(ReadyToDecide, AwaitingDecision), traceIDs...)
	if err != nil {
		return nil, err
	}

	return succeed, nil

}

func (r *RedisBasicStore) ChangeTraceStatus(ctx context.Context, traceIDs []string, fromState, toState CentralTraceState) error {
	ctx, span := r.tracer.Start(ctx, "ChangeTraceStatus")
	defer span.End()

	otelutil.AddSpanFields(span, map[string]interface{}{
		"num_of_traces": len(traceIDs),
		"from_state":    fromState,
		"to_state":      toState,
	})

	conn := r.client.Get()
	defer conn.Close()

	if toState == DecisionDrop {
		traces, err := r.traces.getTraceStatuses(ctx, conn, traceIDs)
		if err != nil {
			return err
		}

		for _, trace := range traces {
			r.decisionCache.Record(trace, false, "")
		}

		if len(traceIDs) == 0 {
			return nil
		}

		succeed, err := r.states.toNextState(ctx, conn, newTraceStateChangeEvent(fromState, toState), traceIDs...)
		if err != nil {
			return err
		}

		// remove span list
		spanListKeys := make([]string, 0, len(traceIDs))
		for _, traceID := range succeed {
			spanListKeys = append(spanListKeys, spansHashByTraceIDKey(traceID))
		}

		_, err = conn.Del(spanListKeys...)
		if err != nil {
			return err
		}

		return nil
	}

	_, err := r.states.toNextState(ctx, conn, newTraceStateChangeEvent(fromState, toState), traceIDs...)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) KeepTraces(ctx context.Context, statuses []*CentralTraceStatus) error {
	ctx, span := r.tracer.Start(ctx, "KeepTraces")
	defer span.End()

	otelutil.AddSpanField(span, "num_of_traces", len(statuses))
	conn := r.client.Get()
	defer conn.Close()

	traceIDs := make([]string, 0, len(statuses))
	for _, status := range statuses {
		traceIDs = append(traceIDs, status.TraceID)
	}

	if len(traceIDs) == 0 {
		return nil
	}

	succeed, err := r.states.toNextState(ctx, conn, newTraceStateChangeEvent(AwaitingDecision, DecisionKeep), traceIDs...)
	if err != nil {
		return err
	}

	var successMap map[string]struct{}
	if len(succeed) != len(traceIDs) {
		successMap = make(map[string]struct{}, len(succeed))
		for _, id := range succeed {
			successMap[id] = struct{}{}
		}
	}

	for _, status := range statuses {
		if successMap != nil {
			if _, ok := successMap[status.TraceID]; !ok {
				continue
			}

		}

		// store keep reason in status
		err := r.traces.keepTrace(ctx, conn, status)
		if err != nil {
			continue
		}
		r.decisionCache.Record(status, true, status.KeepReason)
	}

	// remove span list
	spanListKeys := make([]string, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		spanListKeys = append(spanListKeys, spansHashByTraceIDKey(traceID))
	}

	_, err = conn.Del(spanListKeys...)
	if err != nil {
		return err
	}

	return nil
}

func (r *RedisBasicStore) getTraceState(ctx context.Context, conn redis.Conn, traceID string) (state CentralTraceState) {
	ctx, span := r.tracer.Start(ctx, "getTraceState")
	defer span.End()
	defer otelutil.AddSpanField(span, "state", state)

	otelutil.AddSpanField(span, "trace_id", traceID)
	if tracerec, _, found := r.decisionCache.Test(traceID); found {
		// it was in the decision cache, so we can return the right thing
		if tracerec.Kept() {
			state = DecisionKeep
		} else {
			state = DecisionDrop
		}

		otelutil.AddSpanFields(span, map[string]interface{}{
			"in_cache": true,
		})

		return state
	}

	if r.states.exists(ctx, conn, DecisionDrop, traceID) {
		return DecisionDrop
	}

	if r.states.exists(ctx, conn, DecisionKeep, traceID) {
		return DecisionKeep
	}

	if r.states.exists(ctx, conn, AwaitingDecision, traceID) {
		return AwaitingDecision
	}

	if r.states.exists(ctx, conn, ReadyToDecide, traceID) {
		return ReadyToDecide
	}

	if r.states.exists(ctx, conn, DecisionDelay, traceID) {
		return DecisionDelay
	}

	if r.states.exists(ctx, conn, Collecting, traceID) {
		return Collecting
	}

	return Unknown
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

type centralTraceStatusInit struct {
	TraceID    string
	Rate       uint
	Count      uint32 // number of spans in the trace
	EventCount uint32 // number of span events in the trace
	LinkCount  uint32 // number of span links in the trace
}

type centralTraceStatusReason struct {
	KeepReason  string
	ReasonIndex uint // this is the cache ID for the reason
}

type centralTraceStatusRedis struct {
	TraceID     string
	State       string
	Rate        uint
	Count       uint32
	EventCount  uint32
	LinkCount   uint32
	KeepReason  string
	ReasonIndex uint
}

func normalizeCentralTraceStatusRedis(status *centralTraceStatusRedis) *CentralTraceStatus {
	return &CentralTraceStatus{
		TraceID:     status.TraceID,
		State:       Unknown,
		Rate:        status.Rate,
		reasonIndex: status.ReasonIndex,
		KeepReason:  status.KeepReason,
		Count:       status.Count,
		EventCount:  status.EventCount,
		LinkCount:   status.LinkCount,
	}
}

func (t *tracesStore) addStatus(ctx context.Context, conn redis.Conn, span *CentralSpan) error {
	_, spanStatus := t.tracer.Start(ctx, "addStatus", trace.WithAttributes(attribute.KeyValue{Key: "trace_id", Value: attribute.StringValue(span.TraceID)}))
	defer spanStatus.End()

	trace := &centralTraceStatusInit{
		TraceID: span.TraceID,
	}

	_, err := conn.SetHashTTL(t.traceStatusKey(span.TraceID), trace, expirationForTraceStatus)
	if err != nil {
		return err
	}

	return nil
}

func (t *tracesStore) keepTrace(ctx context.Context, conn redis.Conn, status *CentralTraceStatus) error {
	ctx, span := t.tracer.Start(ctx, "keepTrace")
	defer span.End()

	otelutil.AddSpanField(span, "trace_id", status.TraceID)
	trace := &centralTraceStatusReason{
		KeepReason: status.KeepReason,
	}

	_, err := conn.SetNXHash(t.traceStatusKey(status.TraceID), trace)
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

func (t *tracesStore) getTraceStatuses(ctx context.Context, conn redis.Conn, traceIDs []string) (map[string]*CentralTraceStatus, error) {
	ctx, span := t.tracer.Start(ctx, "getTraceStatuses")
	defer span.End()

	otelutil.AddSpanField(span, "num_of_traces", len(traceIDs))
	if len(traceIDs) == 0 {
		return nil, nil
	}

	statuses := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, traceID := range traceIDs {
		status := &centralTraceStatusRedis{}
		err := conn.GetStructHash(t.traceStatusKey(traceID), status)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve trace status for trace ID %s with error %s", traceID, err)
		}

		statuses[traceID] = normalizeCentralTraceStatusRedis(status)
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
func (t *tracesStore) storeSpan(ctx context.Context, conn redis.Conn, span *CentralSpan) error {
	_, spanStatus := t.tracer.Start(ctx, "storeSpan", trace.WithAttributes(attribute.KeyValue{Key: "trace_id", Value: attribute.StringValue(span.TraceID)}))
	defer spanStatus.End()

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

func (t *tracesStore) incrementSpanCounts(ctx context.Context, conn redis.Conn, traceID string, spanType SpanType) error {
	ctx, span := t.tracer.Start(ctx, "incrementSpanCounts")
	defer span.End()

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
		field = "EventCount"
	case SpanTypeLink:
		field = "LinkCount"
	default:
		field = "Count"
	}

	return redis.NewIncrByHashCommand(t.traceStatusKey(traceID), field, 1), nil
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

type traceStateProcessor struct {
	states       []CentralTraceState
	config       traceStateProcessorConfig
	clock        clockwork.Clock
	reaperTicker clockwork.Ticker
	cancel       context.CancelFunc
}

func newTraceStateProcessor(cfg traceStateProcessorConfig, clock clockwork.Clock) *traceStateProcessor {
	if cfg.reaperRunInterval == 0 {
		cfg.reaperRunInterval = 10 * time.Second
	}
	if cfg.maxTraceRetention == 0 {
		cfg.maxTraceRetention = 24 * time.Hour
	}
	s := &traceStateProcessor{
		states: []CentralTraceState{
			DecisionKeep,
			DecisionDrop,
			AwaitingDecision,
			ReadyToDecide,
			DecisionDelay,
			Collecting,
		},
		config:       cfg,
		clock:        clock,
		reaperTicker: clock.NewTicker(cfg.reaperRunInterval),
	}

	return s
}

func (t *traceStateProcessor) Start(ctx context.Context, redis redis.Client) error {
	if err := ensureValidStateChangeEvents(redis); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	go t.cleanupExpiredTraces(ctx, redis)

	return nil
}

func (t *traceStateProcessor) Stop() {
	if t.cancel != nil {
		t.cancel()
	}

	if t.reaperTicker != nil {
		t.reaperTicker.Stop()
	}
}

// addTrace stores the traceID into a set and insert the current time into
// a list. The list is used to keep track of the time the trace was added to
// the state. The set is used to check if the trace is in the state.
func (t *traceStateProcessor) addNewTrace(ctx context.Context, conn redis.Conn, traceID string) error {
	ctx, span := t.tracer.Start(ctx, "addNewTrace", trace.WithAttributes(attribute.KeyValue{Key: "trace_id", Value: attribute.StringValue(traceID)}))
	defer span.End()
	return t.applyStateChange(ctx, conn, newTraceStateChangeEvent(Unknown, Collecting), traceID)
}

func (t *traceStateProcessor) stateByTraceIDsKey(state CentralTraceState) string {
	return fmt.Sprintf("%s:traces", state)
}

func (t *traceStateProcessor) traceStatesKey(traceID string) string {
	return fmt.Sprintf("%s:states", traceID)
}

func (t *traceStateProcessor) allTraceIDs(ctx context.Context, conn redis.Conn, state CentralTraceState, n int) ([]string, error) {
	ctx, span := t.tracer.Start(ctx, "allTraceIDs")
	defer span.End()

	index := -1
	if n > 0 {
		index = n - 1
	}
	results, err := conn.ZRange(t.stateByTraceIDsKey(state), 0, index)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (t *traceStateProcessor) traceIDsWithTimestamp(ctx context.Context, conn redis.Conn, state CentralTraceState, traceIDs []string) (map[string]time.Time, error) {
	ctx, span := t.tracer.Start(ctx, "traceIDsWithTimestamp")
	defer span.End()

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
		traces[traceIDs[i]] = time.UnixMicro(value)
	}

	return traces, nil
}

func (t *traceStateProcessor) exists(ctx context.Context, conn redis.Conn, state CentralTraceState, traceID string) bool {
	exist, err := conn.ZExist(t.stateByTraceIDsKey(state), traceID)
	if err != nil {
		return false
	}

	return exist
}

func (t *traceStateProcessor) remove(conn redis.Conn, state CentralTraceState, traceIDs ...string) error {
	return conn.ZRemove(t.stateByTraceIDsKey(state), traceIDs)
}

func (t *traceStateProcessor) toNextState(ctx context.Context, conn redis.Conn, changeEvent stateChangeEvent, traceIDs ...string) ([]string, error) {
	ctx, span := t.tracer.Start(ctx, "toNextState")
	defer span.End()

	otelutil.AddSpanFields(span, map[string]interface{}{
		"num_of_traces": len(traceIDs),
		"from_state":    changeEvent.current,
		"to_state":      changeEvent.next,
	})

	succeed := make([]string, 0, len(traceIDs))
	for _, traceID := range traceIDs {
		err := t.applyStateChange(ctx, conn, changeEvent, traceID)
		if err != nil {
			fmt.Println("apply_state_change_error", err)
			continue
		}
		succeed = append(succeed, traceID)
	}
	otelutil.AddSpanField(span, "num_of_succeed", len(succeed))

	if len(succeed) == 0 {
		err := errors.New("failed to apply state change")
		otelutil.AddSpanField(span, "error", err)
		return nil, err
	}

	return succeed, nil
}

// cleanupExpiredTraces removes traces from the state map if they have been in the state for longer than
// the configured time.
func (t *traceStateProcessor) cleanupExpiredTraces(ctx context.Context, redis redis.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.reaperTicker.Chan():
			t.removeExpiredTraces(redis)
		}
	}
}

func (t *traceStateProcessor) removeExpiredTraces(client redis.Client) {
	conn := client.Get()
	defer conn.Close()

	// get the traceIDs that have been in the state for longer than the expiration time
	for _, state := range t.states {
		traceIDs, err := conn.ZRangeByScoreString(t.stateByTraceIDsKey(state), t.clock.Now().Add(-t.config.maxTraceRetention).UnixMicro())
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
func (t *traceStateProcessor) applyStateChange(ctx context.Context, conn redis.Conn, stateChange stateChangeEvent, traceID string) error {
	ctx, span := t.tracer.Start(ctx, "applyStateChange")
	defer span.End()

	otelutil.AddSpanField(span, "trace_id", traceID)
	result, err := t.config.changeState.Do(ctx,
		conn, t.traceStatesKey(traceID), validStateChangeEventsKey,
		stateChange.current.String(), stateChange.next.String(),
		expirationForTraceState.Seconds(), traceID, t.clock.Now().UnixMicro())
	if err != nil {
		otelutil.AddSpanField(span, "error", err.Error())
		return err
	}
	val, ok := result.(int64)
	if !ok {
		return fmt.Errorf("unexpected return value from state change script: %v", result)
	}
	if val != 1 {
		otelutil.AddSpanFields(span, map[string]interface{}{
			"result":   val,
			"is_valid": ok,
		})
		return errors.New("failed to apply state change")
	}
	otelutil.AddSpanFields(span, map[string]interface{}{
		"result": val,
	})

	return nil
}

const stateChangeKey = 2
const stateChangeScript = `
  local traceStateKey = KEYS[1]
  local possibleStateChangeEvents = KEYS[2]
  local previousState = ARGV[1]
  local nextState = ARGV[2]
  local ttl = ARGV[3]
  local traceID = ARGV[4]
  local timestamp = ARGV[5]

  local currentState = redis.call('LINDEX', traceStateKey, -1)
  if (currentState == nil or currentState == false) then
	currentState = previousState
  end

  if (currentState ~= previousState) then
	do return -1 end
  end

  local stateChangeEvent = string.format("%s-%s", currentState, nextState)
  local changeEventIsValid = redis.call('SISMEMBER', possibleStateChangeEvents, stateChangeEvent)
  if (changeEventIsValid == 0) then
    do return -2 end
  end

  redis.call('RPUSH', traceStateKey, nextState)
  redis.call('EXPIRE', traceStateKey, ttl)

  local added = redis.call('ZADD', string.format("%s:traces", nextState), "NX", timestamp, traceID)
  if (added == 0) then
	do return -3 end
  end

  local removed = redis.call('ZREM', string.format("%s:traces", currentState), traceID)
  return 1
`

const validStateChangeEventsKey = "valid-state-change-events"

func ensureValidStateChangeEvents(client redis.Client) error {
	conn := client.Get()
	defer conn.Close()

	return conn.SAdd(validStateChangeEventsKey,
		newTraceStateChangeEvent(Unknown, Collecting).string(),
		newTraceStateChangeEvent(Collecting, DecisionDelay).string(),
		newTraceStateChangeEvent(DecisionDelay, ReadyToDecide).string(),
		newTraceStateChangeEvent(ReadyToDecide, AwaitingDecision).string(),
		newTraceStateChangeEvent(AwaitingDecision, ReadyToDecide).string(),
		newTraceStateChangeEvent(AwaitingDecision, DecisionKeep).string(),
		newTraceStateChangeEvent(AwaitingDecision, DecisionDrop).string(),
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
