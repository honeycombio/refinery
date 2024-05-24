package centralstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/redis"
	"github.com/honeycombio/refinery/types"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/trace"
)

const (
	// if a new refinery is started after the trace has passed its trace retention time,
	// a late span could be sent to the new refinery. The new refinery will not have the trace
	// decision in memory, so it may make a different decision than the original refinery.
	defaultTraceRetention  = 15 * time.Minute
	defaultReaperBatchSize = 400
	redigoTimestamp        = "2006-01-02 15:04:05.999999 -0700 MST"

	metricsKey          = "refinery:metrics"
	traceStatusCountKey = "refinery:trace_status_count"

	metricsPrefixCount      = "redisstore_count_"
	metricsPrefixMemory     = "redisstore_memory_"
	metricsPrefixConnection = "redisstore_conn_"

	decisionKeepInMemoryOnly = "%?"
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

	traces              *tracesStore
	states              *traceStateProcessor
	lastMetricsRecorded time.Time
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
		reaperRunInterval:       time.Duration(opt.ReaperRunInterval),
		reaperBatchSize:         opt.ReaperBatchSize,
		maxTraceRetention:       time.Duration(opt.TraceTimeout * 10),
		changeState:             r.RedisClient.NewScript(stateChangeKey, stateChangeScript),
		getTraceNeedingDecision: r.RedisClient.NewScript(tracesNeedingDecisionScriptKey, tracesNeedingDecisionScript),
		removeExpiredTraces:     r.RedisClient.NewScript(removeExpiredTracesKey, removeExpiredTracesScript),
	}

	stateProcessor := newTraceStateProcessor(stateProcessorCfg, r.Clock, r.Tracer)

	err := stateProcessor.init(r.RedisClient)
	if err != nil {
		return err
	}

	r.traces = newTraceStatusStore(r.Clock, r.Tracer, r.RedisClient.NewScript(keepTraceKey, keepTraceScript), r.Config)
	r.states = stateProcessor

	// register metrics for each state
	for _, state := range r.states.states {
		if state == DecisionDrop || state == DecisionKeep {
			continue
		}
		r.Metrics.Register(metricsPrefixCount+string(state), "gauge")
	}

	// register metrics for connection pool stats
	r.Metrics.Register(metricsPrefixConnection+"active", "gauge")
	r.Metrics.Register(metricsPrefixConnection+"idle", "gauge")
	r.Metrics.Register(metricsPrefixConnection+"wait", "gauge")
	r.Metrics.Register(metricsPrefixConnection+"wait_duration_ms", "gauge")

	if r.Config.GetRedisMetricsCycleRate() != 0 {
		// register metrics for memory stats
		r.Metrics.Register(metricsPrefixMemory+"used_total", "gauge")
		r.Metrics.Register(metricsPrefixMemory+"used_peak", "gauge")
		r.Metrics.Register(metricsPrefixCount+"keys", "gauge")
		r.Metrics.Register(metricsPrefixCount+"traces", "gauge")
	}

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
	r.Metrics.Gauge(metricsPrefixConnection+"wait_duration_ms", connStats.WaitDuration.Milliseconds())

	// don't record metrics if the cycle rate is 0 or if we haven't reached the cycle rate
	if r.Config.GetRedisMetricsCycleRate() == 0 || r.Clock.Now().Sub(r.lastMetricsRecorded) < r.Config.GetRedisMetricsCycleRate() {
		return nil
	}

	r.lastMetricsRecorded = r.Clock.Now()

	conn := r.RedisClient.Get()
	defer conn.Close()

	ok, unlock := conn.AcquireLockWithRetries(ctx, metricsKey, 10*time.Second, 3, 1*time.Second)
	if !ok {
		return nil
	}

	defer unlock()

	for _, state := range r.states.states {
		if state == DecisionDrop || state == DecisionKeep {
			continue
		}
		// get the state counts
		count, err := conn.ZCount(r.states.stateNameKey(state), 0, -1)
		if err != nil {
			return err
		}
		r.Metrics.Gauge(metricsPrefixCount+string(state), count)
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

func (r *RedisBasicStore) WriteSpans(ctx context.Context, spans []*CentralSpan) error {
	ctx, writespan := otelutil.StartSpanMulti(ctx, r.Tracer, "WriteSpans", map[string]interface{}{
		"num_of_spans": len(spans),
	})
	defer writespan.End()

	states := make(map[string]CentralTraceState, len(spans))
	for _, span := range spans {
		states[span.TraceID] = Unknown
	}

	conn := r.RedisClient.Get()
	defer conn.Close()

	err := r.getTraceStates(ctx, conn, states)
	if err != nil {
		return err
	}

	collecting := make(map[string]struct{})
	storeSpans := make([]*CentralSpan, 0, len(spans))
	newSpans := make([]*CentralSpan, 0, len(spans))
	shouldIncrementCounts := make([]*CentralSpan, 0, len(spans))
	for _, span := range spans {
		if span.TraceID == "" {
			continue
		}

		state := states[span.TraceID]

		switch state {
		case DecisionDrop:
			continue
		case DecisionKeep, AwaitingDecision:
			if span.SpanID == "" {
				continue
			}
			shouldIncrementCounts = append(shouldIncrementCounts, span)
			continue
		case Collecting:
			if span.IsRoot {
				collecting[span.TraceID] = struct{}{}
			}
		case DecisionDelay, ReadyToDecide:
		case Unknown:
			newSpans = append(newSpans, span)
		}

		if span.SpanID != "" {
			storeSpans = append(storeSpans, span)
		}

	}

	err = r.traces.incrementSpanCounts(ctx, conn, shouldIncrementCounts)
	if err != nil {
		return err
	}

	err = r.states.addNewTraces(ctx, conn, newSpans)
	if err != nil {
		return err
	}
	err = r.traces.addStatuses(ctx, conn, newSpans)
	if err != nil {
		return err
	}

	ids := make([]string, 0, len(collecting))
	for id := range collecting {
		ids = append(ids, id)
	}

	_, err = r.states.toNextState(ctx, conn, newTraceStateChangeEvent(Collecting, DecisionDelay), ids...)
	if err != nil {
		return err
	}

	err = r.traces.storeSpans(ctx, conn, storeSpans)
	if err != nil {
		return err
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

// GetStatusForTraces returns the status of the traces with the given traceIDs. It only return the status of the trace if a trace's state is one of the statesToCheck.
func (r *RedisBasicStore) GetStatusForTraces(ctx context.Context, traceIDs []string, statesToCheck ...CentralTraceState) ([]*CentralTraceStatus, error) {
	ctx, span := otelutil.StartSpanWith(ctx, r.Tracer, "GetStatusForTraces", "num_traces", len(traceIDs))
	defer span.End()

	conn := r.RedisClient.Get()
	defer conn.Close()

	validStates := make(map[CentralTraceState]struct{}, len(statesToCheck))
	var decisionMade bool
	for _, state := range statesToCheck {
		validStates[state] = struct{}{}
		// is any of the states we are looking for a decision state?
		decisionMade = decisionMade || state == DecisionKeep || state == DecisionDrop
	}

	// if we are looking for decision states, we need to check the decision cache
	// first to see if we have a decision for the traceID.
	// If a trace is in decision drop state, we don't need to fetch the trace status from redis
	// because we don't store any metadata for traces in decision drop state.
	// If a trace is in decision keep state, we need to check if the metadata is stored in redis.
	// This is done through checking the suffix of the reason string in the decision cache.
	// If the suffix is decisionKeepInMemoryOnly, we don't need to fetch the trace status from redis.
	// Otherwise, we need to fetch the trace status from redis in order to get metadata for the trace.
	var requestToRedis []string
	statuses := make([]*CentralTraceStatus, 0)

	if decisionMade {
		requestToRedis = make([]string, 0)
		for _, traceID := range traceIDs {
			record, reason, found := r.DecisionCache.Test(traceID)
			if !found {
				requestToRedis = append(requestToRedis, traceID)
				continue
			}
			status := &CentralTraceStatus{
				TraceID:   traceID,
				Timestamp: r.Clock.Now(),
			}

			if record.Kept() {
				// only decisions made during stress relief doesn't
				// have any metadata stored in redis. Therefore, we
				// don't need to fetch trace status from redis
				if strings.HasSuffix(reason, decisionKeepInMemoryOnly) {
					status.State = DecisionKeep
					status.KeepReason = strings.TrimSuffix(reason, decisionKeepInMemoryOnly)
					status.Rate = record.Rate()
					status.Count = uint32(record.DescendantCount())
					status.EventCount = uint32(record.SpanEventCount())
					status.LinkCount = uint32(record.SpanLinkCount())
					statuses = append(statuses, status)
					continue
				}

				requestToRedis = append(requestToRedis, traceID)
				continue
			}

			status.State = DecisionDrop
			status.Count = uint32(record.DescendantCount())
			status.EventCount = uint32(record.SpanEventCount())
			status.LinkCount = uint32(record.SpanLinkCount())
			statuses = append(statuses, status)
		}
	} else {
		requestToRedis = traceIDs
	}

	statusMapFromRedis, err := r.traces.getTraceStatuses(ctx, r.RedisClient, requestToRedis)
	if err != nil {
		return nil, err
	}

	// grow the slice to avoid reallocation
	statuses = slices.Grow(statuses, len(statusMapFromRedis))

	for _, status := range statusMapFromRedis {
		// only include statuses that are in the statesToCheck list.
		// exception: if a trace decision was made during stress relief, we need to
		// find the trace state from the decision cache instead of redis
		_, ok := validStates[status.State]
		if !ok {
			continue
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

// GetTracesForState returns a list of up to n trace IDs that match the provided status.
// If n is -1, returns all matching traces.
func (r *RedisBasicStore) GetTracesForState(ctx context.Context, state CentralTraceState, n int) ([]string, error) {
	ctx, span := r.Tracer.Start(ctx, "GetTracesForState")
	defer span.End()

	switch state {
	case DecisionDrop, DecisionKeep:
		otelutil.AddSpanField(span, "decision_made", true)
		return nil, nil
	}

	// if the batch size is -1, we want to return all traces
	if n == -1 {
		n = math.MaxInt64
	}

	conn := r.RedisClient.Get()
	defer conn.Close()

	return r.states.randomTraceIDsByState(ctx, conn, state, n)
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

	expirationDuration := time.Duration(r.Config.GetCentralStoreOptions().TraceTimeout)
	if expirationDuration < defaultTraceRetention {
		expirationDuration = defaultTraceRetention
	}

	traceIDs, err := r.states.config.getTraceNeedingDecision.DoStrings(ctx, conn,
		validStateChangeEventsKey, n, expirationDuration.Seconds(), time.Now().UnixMicro())
	if err != nil {
		if errors.Is(err, redis.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}

	if len(traceIDs) == 0 {
		err := errors.New("failed to get traces for needing decisions")
		span.RecordError(err)
		return nil, err
	}

	return traceIDs, nil

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
		traces, err := r.traces.getTraceStatuses(ctx, r.RedisClient, traceIDs)
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
		if successMap != nil {
			if _, ok := successMap[traceID]; !ok {
				continue
			}
		}
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
// The reason is decorated with a suffix to indicate that it is only stored in memory.
func (r *RedisBasicStore) RecordTraceDecision(ctx context.Context, trace *CentralTraceStatus) error {
	_, span := r.Tracer.Start(ctx, "RecordTraceDecision")
	defer span.End()

	var keep bool
	switch trace.State {
	case DecisionKeep:
		keep = true
	case DecisionDrop:
		keep = false
	default:
		return fmt.Errorf("invalid trace state: %s", trace.State)
	}
	reason := trace.KeepReason + decisionKeepInMemoryOnly

	r.DecisionCache.Record(trace, keep, reason)

	return nil
}

func (r *RedisBasicStore) getTraceStates(ctx context.Context, conn redis.Conn, states map[string]CentralTraceState) error {
	ctx, span := r.Tracer.Start(ctx, "getTraceStates")
	defer span.End()

	var cacheHitCount int
	notFound := make([]string, 0, len(states))
	for traceID := range states {
		tracerec, _, found := r.DecisionCache.Test(traceID)
		if !found {
			// if the trace is not in the cache, we need to retrieve its state from redis
			notFound = append(notFound, traceID)
			continue
		}

		cacheHitCount++
		if tracerec.Kept() {
			states[traceID] = DecisionKeep
		} else {
			states[traceID] = DecisionDrop
		}
	}
	otelutil.AddSpanField(span, "cache_hit_count", cacheHitCount)

	if cacheHitCount == len(states) {
		return nil
	}

	results, err := r.traces.getTraceStates(ctx, conn, notFound)
	if err != nil {
		return err
	}

	for id, state := range results {
		states[id] = state
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
	clock           clockwork.Clock
	tracer          trace.Tracer
	keepTraceScript redis.Script
	config          config.Config
}

func newTraceStatusStore(clock clockwork.Clock, tracer trace.Tracer, keepTraceScript redis.Script, cfg config.Config) *tracesStore {
	return &tracesStore{
		clock:           clock,
		tracer:          tracer,
		keepTraceScript: keepTraceScript,
		config:          cfg,
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
		State:           CentralTraceState(status.State),
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

func (t *tracesStore) addStatuses(ctx context.Context, conn redis.Conn, cspans []*CentralSpan) error {
	_, spanStatus := otelutil.StartSpanMulti(ctx, t.tracer, "addStatus", map[string]interface{}{
		"numSpans": len(cspans),
		"isScript": true,
	})
	defer spanStatus.End()

	// default trace retention is 2x the trace timeout + send delay
	// this should be the same as the timeout for spanCache.GetOldTraceIDs
	expirationDuration := time.Duration(2 * (t.config.GetCentralStoreOptions().TraceTimeout + t.config.GetCentralStoreOptions().SendDelay))
	if expirationDuration < defaultTraceRetention {
		expirationDuration = defaultTraceRetention
	}

	commands := make([]redis.Command, 0, 3*len(cspans))
	for _, span := range cspans {
		// prevent storing signaling spans sent from central collector
		// all actual spans should have a spanID
		if span.SpanID == "" {
			continue
		}

		trace := &centralTraceStatusInit{
			TraceID:    span.TraceID,
			SamplerKey: span.samplerSelector,
		}

		traceStatusKey := t.traceStatusKey(span.TraceID)
		args := redis.Args().AddFlat(trace)

		commands = append(commands, redis.NewMultiSetHashCommand(traceStatusKey, args))
		commands = append(commands, redis.NewExpireCommand(traceStatusKey, expirationDuration.Seconds()))
		commands = append(commands, redis.NewINCRCommand(traceStatusCountKey))
	}

	err := conn.Exec(commands...)
	if err != nil {
		spanStatus.RecordError(err)
		return err
	}
	return err
}

func (t *tracesStore) getTraceStates(ctx context.Context, conn redis.Conn, traceIDs []string) (map[string]CentralTraceState, error) {
	_, span := t.tracer.Start(ctx, "getTraceStates")
	defer span.End()

	if len(traceIDs) == 0 {
		return nil, nil
	}

	states := make(map[string]CentralTraceState, len(traceIDs))
	for _, id := range traceIDs {
		cmd := redis.NewGetHashCommand(t.traceStatusKey(id), "State")
		err := cmd.Send(conn)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
	}

	replies, err := conn.ReceiveStrings(len(traceIDs))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	for i, reply := range replies {
		if reply == "" {
			states[traceIDs[i]] = Unknown
			continue
		}

		states[traceIDs[i]] = CentralTraceState(reply)
	}

	return states, nil
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

func (t *tracesStore) getTraceStatuses(ctx context.Context, client redis.Client, traceIDs []string) (map[string]*CentralTraceStatus, error) {
	_, statusSpan := otelutil.StartSpanWith(ctx, t.tracer, "getTraceStatuses", "num_ids", len(traceIDs))
	defer statusSpan.End()

	if len(traceIDs) == 0 {
		return nil, nil
	}

	// We're going to use generics.FanoutToMap to create a set of parallel workers that will farm
	// out the work of getting the status for each traceID.

	// First, calculate the max number of goroutines to use -- half the max active connections configured
	numGoroutines := t.config.GetParallelism()
	otelutil.AddSpanField(statusSpan, "num_goroutines", numGoroutines)

	// Now create a worker factory that will create a worker that will get the status for a traceID.
	// We also want telemetry so we do a little bit of prework and a cleanup function.
	workerFactory := func(i int) (func(traceID string) *CentralTraceStatus, func(int)) {
		conn := client.Get()
		queries := 0
		found := 0
		_, span := otelutil.StartSpanWith(ctx, t.tracer, "getTraceStatusesWorker", "worker", i)
		worker := func(traceID string) *CentralTraceStatus {
			if traceID == "" {
				return nil
			}
			status := &centralTraceStatusRedis{}
			err := conn.GetStructHash(t.traceStatusKey(traceID), status)
			queries++
			if err != nil {
				if errors.Is(err, redis.ErrKeyNotFound) {
					status.TraceID = traceID
					return normalizeCentralTraceStatusRedis(status)
				}
				statusSpan.RecordError(err)
				return nil
			}
			found++
			return normalizeCentralTraceStatusRedis(status)
		}
		cleanup := func(i int) {
			conn.Close()
			otelutil.AddSpanFields(span, map[string]interface{}{
				"num_queries": queries,
				"num_found":   found,
			})
			span.End()
		}
		return worker, cleanup
	}
	// this filters out the nil statuses
	predicate := func(s *CentralTraceStatus) bool {
		return s != nil
	}

	// Now we have all our parts, and this does all the work.
	statuses := generics.FanoutToMap(traceIDs, numGoroutines, workerFactory, predicate)
	otelutil.AddSpanField(statusSpan, "num_statuses", len(statuses))

	// Our result is ready
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
func (t *tracesStore) storeSpans(ctx context.Context, conn redis.Conn, spans []*CentralSpan) error {
	if len(spans) == 0 {
		return nil
	}

	_, spanStore := otelutil.StartSpanMulti(ctx, t.tracer, "storeSpan", map[string]interface{}{
		"isScript": true,
		"numSpans": len(spans),
	})
	defer spanStore.End()

	commands := make([]redis.Command, 2*len(spans))
	for i := 0; i < len(commands); i += 2 {
		var err error
		span := spans[i/2]
		commands[i], err = addToSpanHash(span)
		if err != nil {
			return err
		}

		commands[i+1] = t.incrementSpanCountsCMD(span.TraceID, span.Type)
	}

	err := conn.Exec(commands...)
	if err != nil {
		spanStore.RecordError(err)
	}
	return err
}

func (t *tracesStore) incrementSpanCounts(ctx context.Context, conn redis.Conn, spans []*CentralSpan) error {
	if len(spans) == 0 {
		return nil
	}

	_, spanInc := otelutil.StartSpanMulti(ctx, t.tracer, "incrementSpanCounts", map[string]interface{}{
		"num_spans": len(spans),
		"isScript":  true,
	})
	defer spanInc.End()

	cmds := make([]redis.Command, len(spans))
	for i, s := range spans {
		cmds[i] = t.incrementSpanCountsCMD(s.TraceID, s.Type)
	}

	err := conn.Exec(cmds...)
	if err != nil {
		spanInc.RecordError(err)
	}
	return err
}

func (t *tracesStore) incrementSpanCountsCMD(traceID string, spanType types.SpanType) redis.Command {

	var field string
	switch spanType {
	case types.SpanTypeEvent:
		field = "EventCount"
	case types.SpanTypeLink:
		field = "LinkCount"
	default:
		field = "Count"
	}

	return redis.NewIncrByHashCommand(t.traceStatusKey(traceID), field, 1)
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
	reaperBatchSize   int
	maxTraceRetention time.Duration

	changeState             redis.Script
	getTraceNeedingDecision redis.Script
	removeExpiredTraces     redis.Script
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
	if cfg.reaperBatchSize == 0 {
		cfg.reaperBatchSize = 500
	}
	if cfg.maxTraceRetention < defaultTraceRetention {
		cfg.maxTraceRetention = defaultTraceRetention
	}
	s := &traceStateProcessor{
		states: []CentralTraceState{
			DecisionKeep,
			DecisionDrop,
			Collecting,
			DecisionDelay,
			AwaitingDecision,
			ReadyToDecide,
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
func (t *traceStateProcessor) addNewTraces(ctx context.Context, conn redis.Conn, spans []*CentralSpan) error {
	if len(spans) == 0 {
		return nil
	}

	ctx, span := otelutil.StartSpanWith(ctx, t.tracer, "addNewTraces", "num_traces", len(spans))
	defer span.End()

	ids := make([]string, len(spans))
	for i, s := range spans {
		ids[i] = s.TraceID
	}

	_, err := t.applyStateChange(ctx, conn, newTraceStateChangeEvent(Unknown, Collecting), ids)
	return err
}

func (t *traceStateProcessor) stateNameKey(state CentralTraceState) string {
	return fmt.Sprintf("%s:traces", state)
}

func (t *traceStateProcessor) traceStatesKey(traceID string) string {
	return fmt.Sprintf("%s:states", traceID)
}

// randomTraceIDsByState returns the traceIDs that are in a given trace state no older than the maxTraceRetention.
// If n is not zero, it will return at most n traceIDs.
func (t *traceStateProcessor) randomTraceIDsByState(ctx context.Context, conn redis.Conn, state CentralTraceState, n int) ([]string, error) {
	_, span := t.tracer.Start(ctx, "randomTraceIDsByState")
	defer span.End()

	ids, err := conn.ZRandom(t.stateNameKey(state), n)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	otelutil.AddSpanField(span, "num_ids", len(ids))
	return ids, err
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
	if len(traceIDs) == 0 {
		return nil, nil
	}
	ctx, span := otelutil.StartSpanMulti(ctx, t.tracer, "toNextState", map[string]interface{}{
		"num_traces": len(traceIDs),
		"from_state": changeEvent.current,
		"to_state":   changeEvent.next,
	})
	defer span.End()

	otelutil.AddSpanFields(span, map[string]interface{}{
		"num_of_traces": len(traceIDs),
		"from_state":    changeEvent.current,
		"to_state":      changeEvent.next,
	})

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
			otelutil.AddSpanField(span, "interval", t.config.reaperRunInterval.String())
			t.removeExpiredTraces(ctx, redis)
			span.End()
		}
	}
}

func (t *traceStateProcessor) removeExpiredTraces(ctx context.Context, client redis.Client) {
	ctx, span := otelutil.StartSpanMulti(ctx, t.tracer, "removeExpiredTraces", map[string]interface{}{
		"num_states": len(t.states),
	})
	defer span.End()

	conn := client.Get()
	defer conn.Close()

	// get the traceIDs that have been in the state for longer than the expiration time
	for _, state := range t.states {
		if state == DecisionKeep || state == DecisionDrop {
			continue
		}
		replies, err := t.config.removeExpiredTraces.DoInt(ctx, conn, t.stateNameKey(state),
			t.clock.Now().Add(-t.config.maxTraceRetention).UnixMicro(),
			t.config.reaperBatchSize)

		if err != nil {
			span.RecordError(err)
			continue
		}

		otelutil.AddSpanField(span, state.String(), replies)
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
		t.config.maxTraceRetention.Seconds(), t.clock.Now().UnixMicro()).AddFlat(traceIDs)

	result, err := t.config.changeState.DoStrings(ctx,
		conn, args...)

	if err != nil {
		otelutil.AddSpanField(span, "error", err.Error())
		return nil, err
	}

	if len(result) == 0 {
		err := fmt.Errorf("failed to apply state change %s for traces %v", stateChange.string(), traceIDs)
		span.RecordError(err)
		return nil, err
	}
	otelutil.AddSpanFields(span, map[string]interface{}{
		"result": result,
	})

	return result, nil
}

const tracesNeedingDecisionScriptKey = 1
const tracesNeedingDecisionScript = validStateChangeEventsScript + `
		local batchSize = ARGV[1]
		local ttl = ARGV[2]
		local timestamp = ARGV[3]
		local result = {}
		local previousState = "ready_to_decide"
		local nextState = "awaiting_decision"

	  	local traceIDs = redis.call('ZRANDMEMBER', "ready_to_decide:traces", batchSize)
		if next(traceIDs) == nil then
   			-- myTable is empty
			return -1
		end

  -- iterate through the traceIDs and move them to the next state
  for i, traceID in ipairs(traceIDs) do
    -- unfortunately, Lua doesn't support "continue" statement in for loops.
	-- even though, Lua 5.2+ supports "goto" statement, we can't use it here because
	-- Redis only supports Lua 5.1.
	-- The interior "repeat ... until true" is a way of creating a do-once loop, and "do break end" is a way to
	-- spell "break" that indicates it's not just a normal break.
	-- This is a common pattern to simulate "continue" in Lua versions before 5.2.
  	repeat
` + traceStateChangeScript + `
	   -- add it to the result list
	   table.insert(result, traceID)
	until true
  end


		return result
`

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
const stateChangeScript = validStateChangeEventsScript + `
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
` + traceStateChangeScript + `
	   -- add it to the result list
	   table.insert(result, traceID)
	until true
  end


 return result
`

const traceStateChangeScript = `
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
	   local changeEventIsValid = redis.call('SISMEMBER', validStateChangeEvents, stateChangeEvent)

	   if (changeEventIsValid == 0) then
	     do break end
	   end

	   redis.call('RPUSH', traceStateKey, nextState)
	   redis.call('EXPIRE', traceStateKey, ttl)

	   -- the construction of the key for the sorted set should match with the stateNameKey function
	   -- in the traceStateProcessor struct

	   if (nextState ~= "decision_keep") and (nextState ~= "decision_drop") then
		local added = redis.call('ZADD', string.format("%s:traces", nextState), "NX", timestamp, traceID)
	   end

	   local removed = redis.call('ZREM', string.format("%s:traces", currentState), traceID)

	   local status = redis.call("HSET", string.format("%s:status", traceID), "State", nextState, "Timestamp", timestamp)
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

// validStateChangeEventsScript is a lua script that ensures the valid state change events are present in redis
// If the valid state change events set doesn't exist, it will be created and populated with the valid state change events
// NOTE: the events value should match the string value from stateChangeEvent struct
const validStateChangeEventsScript = `
  local validStateChangeEvents = KEYS[1]

  local eventsExist = redis.call('EXISTS', validStateChangeEvents)
  if (eventsExist ~= 1) then
	local result = redis.call('SADD', validStateChangeEvents, "unknown-collecting",
	"collecting-decision_delay", "decision_delay-ready_to_decide",
	"ready_to_decide-awaiting_decision", "awaiting_decision-ready_to_decide",
	"awaiting_decision-decision_keep", "awaiting_decision-decision_drop")
	if (result == 0) then
	  return redis.error_reply("Failed to add valid state change events")
	end
  end
`

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

const removeExpiredTracesKey = 1
const removeExpiredTracesScript = `
	local stateKey = KEYS[1]
	local expirationTime = ARGV[1]
	local batchSize = ARGV[2]

	local traces = redis.call('ZRANGE', stateKey,
	"-inf", expirationTime, "byscore", "limit", 0, batchSize)

	local result = redis.call('ZREM', stateKey, unpack(traces))
	return result
`
