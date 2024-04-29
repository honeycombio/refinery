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
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/trace"
)

const (
	expirationForTraceStatus   = 24 * time.Hour
	expirationForTraceState    = 24 * time.Hour
	defaultPendingWorkCapacity = 10000
	redigoTimestamp            = "2006-01-02 15:04:05.999999 -0700 MST"

	metricsKey          = "refinery:metrics"
	traceStatusCountKey = "refinery:trace_status_count"

	metricsPrefixCount      = "redisstore_count_"
	metricsPrefixMemory     = "redisstore_memory_"
	metricsPrefixConnection = "redisstore_conn_"
)

var _ BasicStorer = (*RedisBasicStore)(nil)

// RedisBasicStore is an implementation of BasicStorer that uses Redis as the backing store.
type RedisBasicStore struct {
	Config        config.Config        `inject:""`
	Metrics       metrics.Metrics      `inject:"genericMetrics"`
	DecisionCache cache.TraceSentCache `inject:""`
	RedisClient   redis.Client         `inject:"redis"`
	Tracer        trace.Tracer         `inject:"tracer"`
	Clock         clockwork.Clock      `inject:""`

	traces *tracesStore
	states *traceStateProcessor
}

func (r *RedisBasicStore) Start() error {
	if r.Config == nil {
		return errors.New("missing Config injection in RedisBasicStore")
	}

	if r.DecisionCache == nil {
		return errors.New("missing DecisionCache injection in RedisBasicStore")
	}

	if r.RedisClient == nil {
		return errors.New("missing RedisClient injection in RedisBasicStore")
	}

	if r.Tracer == nil {
		return errors.New("missing Tracer injection in RedisBasicStore")
	}

	if r.Clock == nil {
		return errors.New("missing Clock injection in RedisBasicStore")
	}

	opt := r.Config.GetCentralStoreOptions()

	stateProcessorCfg := traceStateProcessorConfig{
		reaperRunInterval: time.Duration(opt.ReaperRunInterval),
		maxTraceRetention: time.Duration(opt.MaxTraceRetention),
		changeState:       r.RedisClient.NewScript(stateChangeKey, stateChangeScript),
	}

	stateProcessor := newTraceStateProcessor(stateProcessorCfg, r.Clock, r.Tracer)

	err := stateProcessor.init(r.RedisClient)
	if err != nil {
		return err
	}

	r.traces = newTraceStatusStore(r.Clock, r.Tracer,
		r.RedisClient.NewScript(keepTraceKey, keepTraceScript),
		r.RedisClient.NewScript(addStatusKey, addStatusScript),
	)
	r.states = stateProcessor

	// register metrics for each state
	for _, state := range r.states.states {
		r.Metrics.Register(metricsPrefixCount+string(state), "gauge")
	}

	// register metrics for connection pool stats
	r.Metrics.Register(metricsPrefixConnection+"active", "gauge")
	r.Metrics.Register(metricsPrefixConnection+"idle", "gauge")
	r.Metrics.Register(metricsPrefixConnection+"wait", "gauge")
	r.Metrics.Register(metricsPrefixConnection+"wait_duration", "histogram")

	// register metrics for memory stats
	r.Metrics.Register(metricsPrefixMemory+"used_total", "gauge")
	r.Metrics.Register(metricsPrefixMemory+"used_peak", "gauge")
	r.Metrics.Register(metricsPrefixCount+"keys", "gauge")
	r.Metrics.Register(metricsPrefixCount+"traces", "gauge")

	return nil
}

func (r *RedisBasicStore) Stop() error {
	r.states.Stop()
	return nil
}

func (r *RedisBasicStore) RecordMetrics(ctx context.Context) error {
	_, span := r.Tracer.Start(ctx, "GetMetrics")
	defer span.End()

	m, err := r.DecisionCache.GetMetrics()
	if err != nil {
		return err
	}

	for k, v := range m {
		r.Metrics.Gauge(k, v)
	}

	// get the connection pool stats from client
	connStats := r.RedisClient.Stats()
	r.Metrics.Gauge(metricsPrefixConnection+"active", connStats.ActiveCount)
	r.Metrics.Gauge(metricsPrefixConnection+"idle", connStats.IdleCount)
	r.Metrics.Gauge(metricsPrefixConnection+"wait", connStats.WaitCount)
	r.Metrics.Histogram(metricsPrefixConnection+"wait_duration", connStats.WaitDuration)

	conn := r.RedisClient.Get()
	defer conn.Close()

	ok, unlock := conn.AcquireLockWithRetries(ctx, metricsKey, 10*time.Second, 3, 1*time.Second)
	if !ok {
		return nil
	}

	defer unlock()

	for _, state := range r.states.states {
		// get the state counts
		traceIDs, err := r.states.traceIDsByState(ctx, conn, state, time.Time{}, time.Time{}, -1)
		if err != nil {
			return err
		}
		r.Metrics.Gauge(metricsPrefixCount+string(state), len(traceIDs))
	}

	count, err := r.traces.count(ctx, conn)
	if err != nil {
		return err
	}
	r.Metrics.Gauge(metricsPrefixCount+"traces", float64(count))

	// If we can't get memory stats from the redis client, we'll just skip it.
	memoryStats, _ := conn.MemoryStats()
	for k, v := range memoryStats {
		switch k {
		case "total.allocated":
			r.Metrics.Gauge(metricsPrefixMemory+"used_total", v)
		case "peak.allocated":
			r.Metrics.Gauge(metricsPrefixMemory+"used_peak", v)
		case "keys.count":
			r.Metrics.Gauge(metricsPrefixCount+"keys", v)
		}
	}

	return nil
}

func (r *RedisBasicStore) WriteSpan(ctx context.Context, span *CentralSpan) error {
	_, writeSpan := r.Tracer.Start(ctx, "WriteSpan")
	defer writeSpan.End()

	conn := r.RedisClient.Get()
	defer conn.Close()

	state := r.getTraceState(ctx, conn, span.TraceID)

	otelutil.AddSpanFields(writeSpan, map[string]interface{}{
		"trace_id":  span.TraceID,
		"span_id":   span.SpanID,
		"span_type": span.Type,
		"state":     state,
	})

	switch state {
	case DecisionDrop:
		return nil
	case DecisionKeep, AwaitingDecision:
		if span.SpanID == "" {
			return nil
		}
		err := r.traces.incrementSpanCounts(ctx, conn, span.TraceID, span.Type)
		if err != nil {
			return err
		}

		return nil
	case Collecting:
		if span.IsRoot {
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

	if span.SpanID != "" {
		err := r.traces.storeSpan(ctx, conn, span)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetTrace returns a CentralTrace with the given traceID.
// if a decision has been made about the trace, the returned value
// will not contain span data.
func (r *RedisBasicStore) GetTrace(ctx context.Context, traceID string) (*CentralTrace, error) {
	_, span := r.Tracer.Start(ctx, "GetTrace")
	defer span.End()

	conn := r.RedisClient.Get()
	defer conn.Close()

	spans := make([]*CentralSpan, 0)
	var tmpSpan []struct {
		SpanID string
		Span   []byte
	}

	err := conn.GetSliceOfStructsHash(spansHashByTraceIDKey(traceID), &tmpSpan)
	if err != nil {
		return nil, err
	}

	errs := make([]error, 0, len(tmpSpan))
	var rootSpan *CentralSpan
	for _, d := range tmpSpan {
		span := &CentralSpan{}
		err := json.Unmarshal(d.Span, span)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		spans = append(spans, span)

		if span.IsRoot {
			rootSpan = span
		}
	}

	return &CentralTrace{
		TraceID: traceID,
		Root:    rootSpan,
		Spans:   spans,
	}, errors.Join(errs...)
}

func (r *RedisBasicStore) GetStatusForTraces(ctx context.Context, traceIDs []string) ([]*CentralTraceStatus, error) {
	ctx, span := otelutil.StartSpanWith(ctx, r.Tracer, "GetStatusForTraces", "num_traces", len(traceIDs))
	defer span.End()

	conn := r.RedisClient.Get()
	defer conn.Close()

	pendingTraceIDs := make([]string, 0, len(traceIDs))
	statusMap := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, id := range traceIDs {
		tracerec, reason, found := r.DecisionCache.Test(id)
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

	statusMapFromRedis, err := r.traces.getTraceStatuses(ctx, conn, traceIDs)
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
		v, ok := statusMapFromRedis[status.TraceID]
		if ok {
			status.SamplerSelector = v.SamplerSelector
			status.Metadata = v.Metadata
			status.Rate = v.Rate
			status.Timestamp = v.Timestamp
		}
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
	ctx, span := r.Tracer.Start(ctx, "GetTracesForState")
	defer span.End()

	switch state {
	case DecisionDrop, DecisionKeep:
		otelutil.AddSpanField(span, "decision_made", true)
		return nil, nil
	}

	conn := r.RedisClient.Get()
	defer conn.Close()

	return r.states.activeTraceIDsByState(ctx, conn, state, -1)
}

// GetTracesNeedingDecision returns a list of up to n trace IDs that are in the
// ReadyForDecision state. These IDs are moved to the AwaitingDecision state
// atomically, so that no other refinery will be assigned the same trace.
func (r *RedisBasicStore) GetTracesNeedingDecision(ctx context.Context, n int) ([]string, error) {
	ctx, span := r.Tracer.Start(ctx, "GetTracesNeedingDecision")
	defer span.End()
	otelutil.AddSpanField(span, "batch_size", n)

	conn := r.RedisClient.Get()
	defer conn.Close()

	traceIDs, err := r.states.activeTraceIDsByState(ctx, conn, ReadyToDecide, n)
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
	ctx, span := r.Tracer.Start(ctx, "ChangeTraceStatus")
	defer span.End()

	otelutil.AddSpanFields(span, map[string]interface{}{
		"num_traces": len(traceIDs),
		"from_state": fromState,
		"to_state":   toState,
	})

	if len(traceIDs) == 0 {
		return nil
	}

	conn := r.RedisClient.Get()
	defer conn.Close()

	if toState == DecisionDrop {
		traces, err := r.traces.getTraceStatuses(ctx, conn, traceIDs)
		if err != nil {
			return err
		}

		for _, trace := range traces {
			r.DecisionCache.Record(trace, false, "")
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
	ctx, span := r.Tracer.Start(ctx, "KeepTraces")
	defer span.End()

	otelutil.AddSpanField(span, "num_traces", len(statuses))
	conn := r.RedisClient.Get()
	defer conn.Close()

	// store keep reason in status
	err := r.traces.keepTrace(ctx, conn, statuses)
	if err != nil {
		return err
	}

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

		r.DecisionCache.Record(status, true, status.KeepReason)
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

// RecordTraceDecision records the decision made about a trace.
// Note: Currently, the decision is only recorded in memory.
func (r *RedisBasicStore) RecordTraceDecision(ctx context.Context, trace *CentralTraceStatus, keep bool, reason string) error {
	_, span := r.Tracer.Start(ctx, "RecordTraceDecision")
	defer span.End()

	if keep {
		r.DecisionCache.Record(trace, keep, reason)
	} else {
		r.DecisionCache.Dropped(trace.ID())
	}

	return nil
}

func (r *RedisBasicStore) getTraceState(ctx context.Context, conn redis.Conn, traceID string) (state CentralTraceState) {
	ctx, span := r.Tracer.Start(ctx, "getTraceState")
	defer span.End()
	defer otelutil.AddSpanField(span, "state", state)

	otelutil.AddSpanField(span, "trace_id", traceID)
	if tracerec, _, found := r.DecisionCache.Test(traceID); found {
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
	clock           clockwork.Clock
	tracer          trace.Tracer
	addStatusScript redis.Script
	keepTraceScript redis.Script
}

func newTraceStatusStore(clock clockwork.Clock, tracer trace.Tracer, keepTraceScript, addStatusScript redis.Script) *tracesStore {
	return &tracesStore{
		clock:           clock,
		tracer:          tracer,
		addStatusScript: addStatusScript,
		keepTraceScript: keepTraceScript,
	}
}

type centralTraceStatusInit struct {
	TraceID    string
	Count      uint32 // number of spans in the trace
	EventCount uint32 // number of span events in the trace
	LinkCount  uint32 // number of span links in the trace
	SamplerKey string
}

type centralTraceStatusReason struct {
	KeepReason  string
	Rate        uint
	ReasonIndex uint // this is the cache ID for the reason
	Metadata    []byte
}

type centralTraceStatusRedis struct {
	TraceID     string
	State       string
	Rate        uint
	Metadata    []byte
	Count       uint32
	EventCount  uint32
	LinkCount   uint32
	KeepReason  string
	SamplerKey  string
	ReasonIndex uint
	Timestamp   int64
}

func normalizeCentralTraceStatusRedis(status *centralTraceStatusRedis) *CentralTraceStatus {
	metadata := make(map[string]any, 0)
	if status.Metadata != nil {
		err := json.Unmarshal(status.Metadata, &metadata)
		if err != nil {
			fmt.Println(err)
		}
	}

	return &CentralTraceStatus{
		TraceID:         status.TraceID,
		State:           Unknown,
		Rate:            status.Rate,
		SamplerSelector: status.SamplerKey,
		reasonIndex:     status.ReasonIndex,
		Metadata:        metadata,
		KeepReason:      status.KeepReason,
		Count:           status.Count,
		EventCount:      status.EventCount,
		LinkCount:       status.LinkCount,
		Timestamp:       time.UnixMicro(status.Timestamp),
	}
}

func (t *tracesStore) addStatus(ctx context.Context, conn redis.Conn, cspan *CentralSpan) error {
	_, spanStatus := otelutil.StartSpanMulti(ctx, t.tracer, "addStatus", map[string]interface{}{
		"trace_id": cspan.TraceID,
		"isScript": true,
	})
	defer spanStatus.End()

	trace := &centralTraceStatusInit{
		TraceID:    cspan.TraceID,
		SamplerKey: cspan.samplerSelector,
	}

	args := redis.Args(t.traceStatusKey(trace.TraceID), traceStatusCountKey, expirationForTraceStatus.Seconds())
	args = args.AddFlat(trace)
	_, err := t.addStatusScript.Do(ctx, conn, args...)
	if err != nil {
		spanStatus.RecordError(err)
		return err
	}

	return nil
}

// keepTrace stores the reason and metadata used for making a keep decision about a trace.
// it updates the trace statuses in batch. If one of the updates fails, it ignores the error
// and continues to update the rest of the traces.
func (t *tracesStore) keepTrace(ctx context.Context, conn redis.Conn, status []*CentralTraceStatus) error {
	_, keepspan := otelutil.StartSpanMulti(ctx, t.tracer, "keepTrace", map[string]interface{}{
		"num_statuses": len(status),
		"isScript":     true,
	})
	defer keepspan.End()

	otelutil.AddSpanField(keepspan, "num_traces", len(status))
	traces := make([]interface{}, 0, len(status)+1)
	traces = append(traces, "key")
	args := redis.Args()
	var err error
	metadata := make([]byte, 0)
	for _, s := range status {
		if s.Metadata != nil {
			metadata, err = json.Marshal(s.Metadata)
			if err != nil {
				keepspan.RecordError(err)
				return err
			}
		}
		trace := &centralTraceStatusReason{
			KeepReason: s.KeepReason,
			Rate:       s.Rate,
			Metadata:   metadata,
		}
		key := map[string]any{"key": t.traceStatusKey(s.TraceID)}

		args = args.AddFlat(key)
		args = args.AddFlat(trace)
		traces = append(traces, args...)

	}

	_, err = t.keepTraceScript.Do(ctx, conn, traces...)
	if err != nil {
		keepspan.RecordError(err)
		return err
	}

	return nil
}

func (t *tracesStore) getTraceStatuses(ctx context.Context, conn redis.Conn, traceIDs []string) (map[string]*CentralTraceStatus, error) {
	_, statusSpan := otelutil.StartSpanWith(ctx, t.tracer, "getTraceStatuses", "num_ids", len(traceIDs))
	defer statusSpan.End()

	if len(traceIDs) == 0 {
		return nil, nil
	}

	statuses := make(map[string]*CentralTraceStatus, len(traceIDs))
	for _, traceID := range traceIDs {
		status := &centralTraceStatusRedis{}
		err := conn.GetStructHash(t.traceStatusKey(traceID), status)
		if err != nil {
			if errors.Is(err, redis.ErrKeyNotFound) {
				continue
			}
			statusSpan.RecordError(err)
			return nil, fmt.Errorf("failed to retrieve trace status for trace ID %s with error %s", traceID, err)
		}

		statuses[traceID] = normalizeCentralTraceStatusRedis(status)
	}

	return statuses, nil
}

func (t *tracesStore) traceStatusKey(traceID string) string {
	return traceID + ":status"
}

// count returns the number of traces in the store.
func (t *tracesStore) count(ctx context.Context, conn redis.Conn) (int64, error) {
	_, span := t.tracer.Start(ctx, "Count")
	defer span.End()

	// read the value from trace status count key
	return conn.GetInt64(traceStatusCountKey)
}

// storeSpan stores the span in the spans hash and increments the span count for the trace.
func (t *tracesStore) storeSpan(ctx context.Context, conn redis.Conn, span *CentralSpan) error {
	_, spanStore := otelutil.StartSpanMulti(ctx, t.tracer, "storeSpan", map[string]interface{}{
		"trace_id":  span.TraceID,
		"span_type": span.Type,
		"isScript":  true,
	})
	defer spanStore.End()

	var err error
	commands := make([]redis.Command, 2)
	commands[0], err = addToSpanHash(span)
	if err != nil {
		spanStore.RecordError(err)
		return err
	}

	commands[1], err = t.incrementSpanCountsCMD(span.TraceID, span.Type)
	if err != nil {
		spanStore.RecordError(err)
		return err
	}

	err = conn.Exec(commands...)
	if err != nil {
		spanStore.RecordError(err)
	}
	return err
}

func (t *tracesStore) incrementSpanCounts(ctx context.Context, conn redis.Conn, traceID string, spanType types.SpanType) error {
	_, spanInc := otelutil.StartSpanMulti(ctx, t.tracer, "incrementSpanCounts", map[string]interface{}{
		"trace_id":  traceID,
		"span_type": spanType,
		"isScript":  true,
	})
	defer spanInc.End()

	cmd, err := t.incrementSpanCountsCMD(traceID, spanType)
	if err != nil {
		spanInc.RecordError(err)
		return err
	}

	_, err = conn.Do(cmd.Name(), cmd.Args()...)
	if err != nil {
		spanInc.RecordError(err)
	}
	return err
}

func (t *tracesStore) incrementSpanCountsCMD(traceID string, spanType types.SpanType) (redis.Command, error) {

	var field string
	switch spanType {
	case types.SpanTypeEvent:
		field = "EventCount"
	case types.SpanTypeLink:
		field = "LinkCount"
	default:
		field = "Count"
	}

	return redis.NewIncrByHashCommand(t.traceStatusKey(traceID), field, 1), nil
}

func spansHashByTraceIDKey(traceID string) string {
	return traceID + ":spans"
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
	states []CentralTraceState
	config traceStateProcessorConfig
	clock  clockwork.Clock
	done   chan struct{}

	tracer trace.Tracer
}

func newTraceStateProcessor(cfg traceStateProcessorConfig, clock clockwork.Clock, tracer trace.Tracer) *traceStateProcessor {
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
		config: cfg,
		clock:  clock,
		done:   make(chan struct{}),
		tracer: tracer,
	}

	return s
}

// init ensures that the valid state change events are stored in a set in redis
// and starts a goroutine to clean up expired traces.
func (t *traceStateProcessor) init(redis redis.Client) error {
	if err := ensureValidStateChangeEvents(redis); err != nil {
		return err
	}

	go t.cleanupExpiredTraces(redis)

	return nil
}

func (t *traceStateProcessor) Stop() {
	if t.done != nil {
		close(t.done)
	}
}

// addTrace stores the traceID into a set and insert the current time into
// a list. The list is used to keep track of the time the trace was added to
// the state. The set is used to check if the trace is in the state.
func (t *traceStateProcessor) addNewTrace(ctx context.Context, conn redis.Conn, traceID string) error {
	ctx, span := otelutil.StartSpanWith(ctx, t.tracer, "addNewTrace", "trace_id", traceID)
	defer span.End()

	_, err := t.applyStateChange(ctx, conn, newTraceStateChangeEvent(Unknown, Collecting), []string{traceID})
	return err
}

func (t *traceStateProcessor) stateNameKey(state CentralTraceState) string {
	return fmt.Sprintf("%s:traces", state)
}

func (t *traceStateProcessor) traceStatesKey(traceID string) string {
	return fmt.Sprintf("%s:states", traceID)
}

// activeTraceIDsByState returns the traceIDs that are in a given trace state no older than the maxTraceRetention.
// If n is not zero, it will return at most n traceIDs.
func (t *traceStateProcessor) activeTraceIDsByState(ctx context.Context, conn redis.Conn, state CentralTraceState, n int) ([]string, error) {
	_, span := t.tracer.Start(ctx, "activeTraceIDsByState")
	defer span.End()

	minTimestamp := t.clock.Now().Add(-t.config.maxTraceRetention)
	ids, err := t.traceIDsByState(ctx, conn, state, minTimestamp, time.Time{}, n)
	if err != nil {
		span.RecordError(err)
	}
	otelutil.AddSpanField(span, "num_ids", len(ids))
	return ids, err
}

// traceIDsByState returns the traceIDs that are in a given trace state.
// If startTime is not zero, it will return traceIDs that have been in the state since startTime.
// If endTime is not zero, it will return traceIDs that have been in the state until endTime.
// If n is not zero, it will return at most n traceIDs.
func (t *traceStateProcessor) traceIDsByState(ctx context.Context, conn redis.Conn, state CentralTraceState, startTime time.Time, endTime time.Time, n int) ([]string, error) {
	_, span := t.tracer.Start(ctx, "traceIDsByState")
	defer span.End()

	start := startTime.UnixMicro()
	if startTime.IsZero() {
		start = 0
	}

	end := endTime.UnixMicro()
	if endTime.IsZero() {
		end = 0
	}

	results, err := conn.ZRangeByScoreString(t.stateNameKey(state), start, end, n, 0)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	otelutil.AddSpanFields(span, map[string]interface{}{
		"cmd":     "ZRANGEBYSCORE",
		"state":   state,
		"num_ids": len(results),
		"start":   start,
		"end":     end,
		"n":       n,
	})
	return results, nil
}

func (t *traceStateProcessor) traceIDsWithTimestamp(ctx context.Context, conn redis.Conn, state CentralTraceState, traceIDs []string) (map[string]time.Time, error) {
	_, span := otelutil.StartSpanMulti(ctx, t.tracer, "traceIDsWithTimestamp", map[string]interface{}{
		"state":   state,
		"cmd":     "ZMSCORE",
		"num_ids": len(traceIDs),
	})
	defer span.End()

	timestamps, err := conn.ZMScore(t.stateNameKey(state), traceIDs)
	if err != nil {
		span.RecordError(err)
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
	_, span := otelutil.StartSpanMulti(ctx, t.tracer, "exists", map[string]interface{}{
		"trace_id": traceID,
		"state":    state,
		"exists":   false,
		"cmd":      "ZEXIST",
	})
	defer span.End()

	exist, err := conn.ZExist(t.stateNameKey(state), traceID)
	if err != nil {
		return false
	}

	otelutil.AddSpanField(span, "exists", exist)

	return exist
}

func (t *traceStateProcessor) remove(ctx context.Context, conn redis.Conn, state CentralTraceState, traceIDs ...string) error {
	_, span := otelutil.StartSpanMulti(ctx, t.tracer, "remove", map[string]interface{}{
		"state":      state,
		"cmd":        "ZREMOVE",
		"num_traces": len(traceIDs),
	})
	defer span.End()
	if len(traceIDs) == 0 {
		return nil
	}

	return conn.ZRemove(t.stateNameKey(state), traceIDs)
}

func (t *traceStateProcessor) toNextState(ctx context.Context, conn redis.Conn, changeEvent stateChangeEvent, traceIDs ...string) ([]string, error) {
	ctx, span := otelutil.StartSpanMulti(ctx, t.tracer, "toNextState", map[string]interface{}{
		"num_traces": len(traceIDs),
		"from_state": changeEvent.current,
		"to_state":   changeEvent.next,
	})
	defer span.End()

	return t.applyStateChange(ctx, conn, changeEvent, traceIDs)
}

// cleanupExpiredTraces removes traces from the state map if they have been in the state for longer than
// the configured time.
func (t *traceStateProcessor) cleanupExpiredTraces(redis redis.Client) {
	ticker := t.clock.NewTicker(t.config.reaperRunInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.done:
			return
		case <-ticker.Chan():
			// cannot defer here!
			ctx, span := t.tracer.Start(context.Background(), "cleanupExpiredTraces")
			t.removeExpiredTraces(ctx, redis)
			span.End()
		}
	}
}

func (t *traceStateProcessor) removeExpiredTraces(ctx context.Context, client redis.Client) {
	ctx, span := otelutil.StartSpanMulti(ctx, t.tracer, "removeExpiredTraces", map[string]interface{}{
		"num_states": len(t.states),
		"cmd":        "ZRANGEBYSCORE",
	})
	defer span.End()

	conn := client.Get()
	defer conn.Close()

	// get the traceIDs that have been in the state for longer than the expiration time
	for _, state := range t.states {
		traceIDs, err := conn.ZRangeByScoreString(t.stateNameKey(state), 0, t.clock.Now().Add(-t.config.maxTraceRetention).UnixMicro(), 100, 0)
		if err != nil {
			span.RecordError(err)
			otelutil.AddSpanFields(span, map[string]interface{}{
				"state": state,
				"error": err.Error(),
			})
			return
		}

		// remove the traceIDs from the state map
		err = t.remove(ctx, conn, state, traceIDs...)
		if err != nil {
			continue
		}
	}

}

// applyStateChange runs a lua script that atomically moves traces between states and returns the trace IDs that has completed a state change.
func (t *traceStateProcessor) applyStateChange(ctx context.Context, conn redis.Conn, stateChange stateChangeEvent, traceIDs []string) ([]string, error) {
	ctx, span := t.tracer.Start(ctx, "applyStateChange")
	defer span.End()

	otelutil.AddSpanField(span, "num_traces", len(traceIDs))

	if len(traceIDs) == 0 {
		return nil, nil
	}

	args := redis.Args(validStateChangeEventsKey, stateChange.current.String(), stateChange.next.String(),
		expirationForTraceState.Seconds(), t.clock.Now().UnixMicro()).AddFlat(traceIDs)

	result, err := t.config.changeState.DoStrings(ctx,
		conn, args...)

	if err != nil {
		otelutil.AddSpanField(span, "error", err.Error())
		return nil, err
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("failed to apply state change %s for traces %v", stateChange.string(), traceIDs)
	}
	otelutil.AddSpanFields(span, map[string]interface{}{
		"result": result,
	})

	return result, nil
}

// stateChangeScript is a lua script that atomically moves traces between states and returns
// the trace IDs that has completed a state change.
// It takes the following arguments:
// KEYS[1] - the set of valid state change events
// ARGV[1] - the previous state. This is the current state submmited by the client
// ARGV[2] - the next state
// ARGV[3] - the expiration time for the state
// ARGV[4] - the current time
// The rest of the arguments are the traceIDs to move between states.

// The script works as follows:
// 1. For each traceID, get the current state from its trace state list. If it doesn't exist yet, use the previous state
// 2. If the current state doesn't match with the previous state, that means the state for the trace has changed
// the current state is no longer valid, so we should abort
// 3. If the current state matches with the state submitted by the client, check if the state change event is valid
// 4. If the state change event is valid, add the trace to the next state sorted set and remove it from the current state sorted set
const stateChangeKey = 1
const stateChangeScript = `
  local possibleStateChangeEvents = KEYS[1]
  local previousState = ARGV[1]
  local nextState = ARGV[2]
  local ttl = ARGV[3]
  local timestamp = ARGV[4]
  local result = {}

  -- iterate through the traceIDs and move them to the next state
  for i, traceID in ipairs(ARGV) do
    -- unfortunately, Lua doesn't support "continue" statement in for loops.
	-- even though, Lua 5.2+ supports "goto" statement, we can't use it here because
	-- Redis only supports Lua 5.1.
	-- The interior "repeat ... until true" is a way of creating a do-once loop, and "do break end" is a way to
	-- spell "break" that indicates it's not just a normal break.
	-- This is a common pattern to simulate "continue" in Lua versions before 5.2.
  	repeat
		-- the first 4 arguments are not traceIDs, so skip them
	    if i < 5 then
		  do break end
		end

		--  get current state for the trace. If it doesn't exist yet, use the previous state
		-- this formatting logic should match with the traceStatesKey function in the traceStateProcessor struct
		local traceStateKey = string.format("%s:states", traceID)
	    local currentState = redis.call('LINDEX', traceStateKey, -1)
	    if (currentState == nil or currentState == false) then
	 	  currentState = previousState
	    end

	   -- if the current state doesn't match with the previous state, that means the state change is
	   --  no longer valid, so we should abort
	   if (currentState ~= previousState) then
	 	  do break end
	   end

	   -- check if the state change event is valid
	   -- this formatting logic should match with the formatting for the stateChangeEvent struct
	   local stateChangeEvent = string.format("%s-%s", currentState, nextState)
	   local changeEventIsValid = redis.call('SISMEMBER', possibleStateChangeEvents, stateChangeEvent)

	   if (changeEventIsValid == 0) then
	     do break end
	   end

	   redis.call('RPUSH', traceStateKey, nextState)
	   redis.call('EXPIRE', traceStateKey, ttl)

	   -- the construction of the key for the sorted set should match with the stateNameKey function
	   -- in the traceStateProcessor struct
	   local added = redis.call('ZADD', string.format("%s:traces", nextState), "NX", timestamp, traceID)

	   local removed = redis.call('ZREM', string.format("%s:traces", currentState), traceID)

	   local status = redis.call("HSET", string.format("%s:status", traceID), "Timestamp", timestamp)

	   -- add it to the result list
	   table.insert(result, traceID)
	until true
  end


 return result
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

// string returns a string representation of the state change event
// this formatting logic should match the one in the stateChange lua script
func (s stateChangeEvent) string() string {
	return s.current.String() + "-" + s.next.String()
}

// addStatusScript automatically creates a new hash with the trace status for
// the traceID and increments the trace count.
const addStatusKey = 2
const addStatusScript = `
	local traceStatusKey = KEYS[1]
	local traceStatusCountKey = KEYS[2]
	local ttl = ARGV[1]

	local traceStatus = {}
	for i=2, table.getn(ARGV) do
    	traceStatus[i-1] = ARGV[i]
    end
    local res = redis.call("HMSET", traceStatusKey, unpack(traceStatus))
	if (res == 0) then
		return -1
	end
    redis.call("EXPIRE", KEYS[1], ARGV[1])

  	-- increment the trace count
	redis.call('INCR', traceStatusCountKey)
	return 1
`

const keepTraceKey = 1
const keepTraceScript = `
	local totalArgs = table.getn(ARGV)
	local traceStatusKey = ""

	for i=1, totalArgs, 2 do
	--	If trace status does not exist, a new entry is created.
	--	If KeepReason already exists, this operation has no effect.
		if ARGV[i] == "key" then
			traceStatusKey = ARGV[i+1]
		else
			redis.call("HSETNX", traceStatusKey, ARGV[i], ARGV[i+1])
		end
	end

	return 1
`
