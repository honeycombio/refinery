package collect

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/types"
)

const collectorLoopHealthPrefix = "collector-loop-"

type sendEarly struct {
	wg          *sync.WaitGroup
	bytesToSend int
}

// CollectLoop represents a single concurrent collection loop that processes
// a subset of the trace ID space. Each loop has its own cache and channels
// but shares configuration and transmission resources with the parent collector.
type CollectLoop struct {
	// ID identifies this particular loop instance
	ID int

	// parent is a reference to the parent InMemCollector for accessing shared resources
	parent *InMemCollector

	// Input channels specific to this loop
	incoming chan *types.Span
	fromPeer chan *types.Span

	// Control signal for memory overages
	sendEarly chan sendEarly

	// Optional pause signal to stop all work. Blocks on receiving a channel, then
	// waits for the received channel to unblock, then resumes.
	pause chan chan struct{}

	// Channel for config reload signals
	reload chan struct{}

	// Our local trace cache. This deliberatly does not have a mutex around it;
	// there is no way to safely access this directly from the outside.
	cache cache.Cache

	// Our local samplers.
	datasetSamplers map[string]sample.Sampler

	// For reporting cache size asynchronously.
	lastCacheSize atomic.Int64
}

// NewCollectLoop creates a new CollectLoop instance
func NewCollectLoop(
	id int,
	parent *InMemCollector,
	incomingSize int,
	peerSize int,
) *CollectLoop {
	return &CollectLoop{
		ID:        id,
		parent:    parent,
		cache:     cache.NewInMemCache(parent.Metrics, parent.Logger),
		incoming:  make(chan *types.Span, incomingSize),
		fromPeer:  make(chan *types.Span, peerSize),
		sendEarly: make(chan sendEarly, 1),

		// Important that this be unbuffered, so the sender blocks until the
		// signal has been received.
		pause: make(chan chan struct{}),

		// Initialize local sampler cache and reload channel
		datasetSamplers: make(map[string]sample.Sampler),
		reload:          make(chan struct{}, 1),
	}
}

// addSpan adds a span to this loop's incoming channel
func (cl *CollectLoop) addSpan(sp *types.Span) error {
	if cl.parent.BlockOnAddSpan {
		cl.incoming <- sp
		cl.parent.Metrics.Increment("span_received")
		cl.parent.Metrics.Up("spans_waiting")
		return nil
	}

	select {
	case cl.incoming <- sp:
		cl.parent.Metrics.Increment("span_received")
		cl.parent.Metrics.Up("spans_waiting")
		return nil
	default:
		return ErrWouldBlock
	}
}

// addSpanFromPeer adds a span from a peer to this loop's peer channel
func (cl *CollectLoop) addSpanFromPeer(sp *types.Span) error {
	if cl.parent.BlockOnAddSpan {
		cl.fromPeer <- sp
		cl.parent.Metrics.Increment("span_received")
		cl.parent.Metrics.Up("spans_waiting")
		return nil
	}

	select {
	case cl.fromPeer <- sp:
		cl.parent.Metrics.Increment("span_received")
		cl.parent.Metrics.Up("spans_waiting")
		return nil
	default:
		return ErrWouldBlock
	}
}

// collect is the main event processing loop for this CollectLoop
func (cl *CollectLoop) collect() {
	defer cl.parent.collectLoopsWG.Done()

	healthKey := collectorLoopHealthPrefix + strconv.Itoa(cl.ID)
	cl.parent.Health.Register(healthKey, cl.parent.Config.GetHealthCheckTimeout())
	defer cl.parent.Health.Unregister(healthKey)

	tickerDuration := cl.parent.Config.GetTracesConfig().GetSendTickerValue()
	ticker := cl.parent.Clock.NewTicker(tickerDuration)
	defer ticker.Stop()

	cl.parent.Health.Ready(healthKey, true)
	for {
		ctx, span := otelutil.StartSpanWith(context.Background(), cl.parent.Tracer, "collect_loop", "loop_id", cl.ID)
		startTime := cl.parent.Clock.Now()

		// Always drain peer channel before doing anything else. By processing peer
		// traffic preferentially we avoid the situation where the cluster essentially
		// deadlocks because peers are waiting to get their events handed off to each
		// other.
		select {
		case sp, ok := <-cl.fromPeer:
			if !ok {
				// channel's been closed; we should shut down.
				span.End()
				return
			}
			cl.processSpan(ctx, sp)
		default:
			select {
			case <-ticker.Chan():
				cl.parent.Health.Ready(healthKey, true)
				cl.sendExpiredTracesInCache(ctx, cl.parent.Clock.Now())

				// Note latest cache size for GetCacheSize()
				cacheSize := cl.cache.GetCacheEntryCount()
				cl.lastCacheSize.Store(int64(cacheSize))
			case sp, ok := <-cl.incoming:
				if !ok {
					// channel's been closed; we should shut down.
					span.End()
					return
				}
				cl.processSpan(ctx, sp)
			case sp, ok := <-cl.fromPeer:
				if !ok {
					// channel's been closed; we should shut down.
					span.End()
					return
				}
				cl.processSpan(ctx, sp)
			case sendEarly := <-cl.sendEarly:
				cl.sendTracesEarly(ctx, sendEarly.bytesToSend)
				sendEarly.wg.Done()
			case <-cl.reload:
				// Clear samplers on config reload
				clear(cl.datasetSamplers)
			case ch := <-cl.pause:
				// We got a pause signal, wait until it unblocks.
				<-ch
			}
		}

		cl.parent.Metrics.Histogram("collector_collect_loop_duration_ms", float64(cl.parent.Clock.Since(startTime).Milliseconds()))
		span.End()
	}
}

// processSpan handles a single span, adding it to the cache or forwarding it
func (cl *CollectLoop) processSpan(ctx context.Context, sp *types.Span) {
	ctx, span := otelutil.StartSpanWith(ctx, cl.parent.Tracer, "processSpan", "loop_id", cl.ID)
	defer func() {
		cl.parent.Metrics.Increment("span_processed")
		cl.parent.Metrics.Down("spans_waiting")
		span.End()
	}()

	tcfg := cl.parent.Config.GetTracesConfig()

	trace := cl.cache.Get(sp.TraceID)
	if trace == nil {
		// if the trace has already been sent, just pass along the span
		if sr, keptReason, found := cl.parent.sampleTraceCache.CheckSpan(sp); found {
			cl.parent.Metrics.Increment("trace_sent_cache_hit")
			// bump the count of records on this trace -- if the root span isn't
			// the last late span, then it won't be perfect, but it will be better than
			// having none at all
			cl.parent.dealWithSentTrace(ctx, sr, keptReason, sp)
			return
		}

		// trace hasn't already been sent (or this span is really old); let's
		// create a new trace to hold it
		cl.parent.Metrics.Increment("trace_accepted")

		timeout := tcfg.GetTraceTimeout()
		if timeout == 0 {
			timeout = 60 * time.Second
		}

		now := cl.parent.Clock.Now()

		trace = &types.Trace{
			APIHost:     sp.APIHost,
			APIKey:      sp.APIKey,
			Dataset:     sp.Dataset,
			Environment: sp.Environment,
			TraceID:     sp.TraceID,
			ArrivalTime: now,
			SendBy:      now.Add(timeout),
		}
		trace.SetSampleRate(sp.SampleRate) // if it had a sample rate, we want to keep it
		// push this into the cache
		cl.cache.Set(trace)
	}
	// if the trace we got back from the cache has already been sent, deal with the
	// span.
	if trace.Sent {
		if sr, reason, found := cl.parent.sampleTraceCache.CheckSpan(sp); found {
			cl.parent.Metrics.Increment("trace_sent_cache_hit")
			cl.parent.dealWithSentTrace(ctx, sr, reason, sp)
			return
		}
		// trace has already been sent, but this is not in the sent cache.
		// we will just use the default late span reason as the sent reason which is
		// set inside the dealWithSentTrace function
		cl.parent.dealWithSentTrace(ctx, cache.NewKeptTraceCacheEntry(trace), "", sp)
		return
	}

	// great! trace is live. add the span.
	trace.AddSpan(sp)

	// we may override these values in conditions below
	var markTraceForSending bool
	timeout := tcfg.GetSendDelay()
	if timeout == 0 {
		timeout = 2 * time.Second // a sensible default
	}

	// if this is a root span, say so and send the trace
	if sp.IsRoot {
		markTraceForSending = true
		trace.RootSpan = sp
	}

	// if the span count has exceeded our SpanLimit, send the trace immediately
	if tcfg.SpanLimit > 0 && uint(trace.DescendantCount()) > tcfg.SpanLimit {
		markTraceForSending = true
		timeout = 0 // don't use a timeout in this case; this is an "act fast" situation
	}

	if markTraceForSending {
		updatedSendBy := cl.parent.Clock.Now().Add(timeout)
		// if the trace has already timed out, we should not update the send_by time
		if trace.SendBy.After(updatedSendBy) {
			trace.SendBy = updatedSendBy
			cl.cache.Set(trace)
		}
	}
}

// sendExpiredTracesInCache finds and sends traces that have exceeded their timeout
func (cl *CollectLoop) sendExpiredTracesInCache(ctx context.Context, now time.Time) {
	ctx, span := otelutil.StartSpanWith(ctx, cl.parent.Tracer, "sendExpiredTracesInCache", "loop_id", cl.ID)
	startTime := cl.parent.Clock.Now()
	defer func() {
		cl.parent.Metrics.Histogram("collector_send_expired_traces_in_cache_dur_ms", float64(cl.parent.Clock.Since(startTime).Milliseconds()))
		span.End()
	}()

	traces := cl.cache.TakeExpiredTraces(now, int(cl.parent.Config.GetTracesConfig().MaxExpiredTraces), nil)

	dur := cl.parent.Clock.Since(startTime)

	span.SetAttributes(
		attribute.Int("num_traces_to_expire", len(traces)),
		attribute.Int64("take_expired_traces_duration_ms", dur.Milliseconds()),
	)

	spanLimit := uint32(cl.parent.Config.GetTracesConfig().SpanLimit)

	var totalSpansSent int64

	for _, t := range traces {
		ctx, sendExpiredTraceSpan := otelutil.StartSpan(ctx, cl.parent.Tracer, "sendExpiredTrace")
		totalSpansSent += int64(t.DescendantCount())

		if t.RootSpan != nil {
			tr, err := cl.makeDecision(ctx, t, TraceSendGotRoot)
			if err != nil {
				sendExpiredTraceSpan.End()
				continue
			}
			cl.parent.send(ctx, tr)
		} else {
			if spanLimit > 0 && t.DescendantCount() > spanLimit {
				tr, err := cl.makeDecision(ctx, t, TraceSendSpanLimit)
				if err != nil {
					sendExpiredTraceSpan.End()
					continue
				}
				cl.parent.send(ctx, tr)
			} else {
				tr, err := cl.makeDecision(ctx, t, TraceSendExpired)
				if err != nil {
					sendExpiredTraceSpan.End()
					continue
				}
				cl.parent.send(ctx, tr)
			}
		}
		sendExpiredTraceSpan.End()
	}

	span.SetAttributes(attribute.Int64("total_spans_sent", totalSpansSent))
}

func (cl *CollectLoop) sendTracesEarly(ctx context.Context, sendEarlyBytes int) {
	// The size of the cache exceeds the user's intended allocation, so we're going to
	// remove the traces from the cache that have had the most impact on allocation.
	// To do this, we sort the traces by their CacheImpact value and then remove traces
	// until the total size is less than the amount to which we want to shrink.
	allTraces := cl.cache.GetAll()

	traceTimeout := cl.parent.Config.GetTracesConfig().GetTraceTimeout()
	if traceTimeout == 0 {
		traceTimeout = 60 * time.Second
	}

	// Sort traces by CacheImpact, heaviest first
	sort.Slice(allTraces, func(i, j int) bool {
		return allTraces[i].CacheImpact(traceTimeout) > allTraces[j].CacheImpact(traceTimeout)
	})

	totalDataSizeSent := 0
	tracesSent := generics.NewSet[string]()
	// Send the traces we can't keep.
	for _, trace := range allTraces {
		t, err := cl.makeDecision(ctx, trace, TraceSendEjectedMemsize)
		if err != nil {
			continue
		}
		tracesSent.Add(trace.TraceID)
		totalDataSizeSent += trace.DataSize
		cl.parent.send(ctx, t)
		if totalDataSizeSent > sendEarlyBytes {
			break
		}
	}
	cl.cache.RemoveTraces(tracesSent)

	cl.lastCacheSize.Store(int64(cl.cache.GetCacheEntryCount()))
}

// GetCacheSize returns the most recently recorded count of traces in this loop's cache
func (cl *CollectLoop) GetCacheSize() int {
	return int(cl.lastCacheSize.Load())
}

func (cl *CollectLoop) makeDecision(ctx context.Context, trace *types.Trace, sendReason string) (s sendableTrace, err error) {
	if trace.Sent {
		return s, errors.New("trace already sent")
	}

	ctx, span := otelutil.StartSpan(ctx, cl.parent.Tracer, "makeDecision")
	defer span.End()
	cl.parent.Metrics.Histogram("trace_span_count", float64(trace.DescendantCount()))

	otelutil.AddSpanFields(span, map[string]interface{}{
		"trace_id": trace.ID(),
		"root":     trace.RootSpan,
		"send_by":  trace.SendBy,
		"arrival":  trace.ArrivalTime,
	})

	var sampler sample.Sampler
	var found bool
	// get sampler key (dataset for legacy keys, environment for new keys)
	samplerSelector := cl.parent.Config.DetermineSamplerKey(trace.APIKey, trace.Environment, trace.Dataset)

	// use sampler key to find sampler; create and cache if not found
	if sampler, found = cl.datasetSamplers[samplerSelector]; !found {
		sampler = cl.parent.SamplerFactory.GetSamplerImplementationForKey(samplerSelector)
		cl.datasetSamplers[samplerSelector] = sampler
	}

	// prepopulate spans with key fields
	allFields, nonRootFields := sampler.GetKeyFields()
	for _, sp := range trace.GetSpans() {
		if sp.IsRoot {
			sp.Data.MemoizeFields(allFields...)
		} else {
			sp.Data.MemoizeFields(nonRootFields...)
		}
	}

	startGetSampleRate := cl.parent.Clock.Now()
	// make sampling decision and update the trace
	rate, shouldSend, reason, key := sampler.GetSampleRate(trace)
	cl.parent.Metrics.Histogram("get_sample_rate_duration_ms", float64(time.Since(startGetSampleRate).Milliseconds()))

	trace.SetSampleRate(rate)
	trace.KeepSample = shouldSend
	// This will observe sample rate attempts even if the trace is dropped
	cl.parent.Metrics.Histogram("trace_aggregate_sample_rate", float64(rate))

	cl.parent.sampleTraceCache.Record(trace, shouldSend, reason)

	var hasRoot bool
	if trace.RootSpan != nil {
		cl.parent.Metrics.Increment("trace_send_has_root")
		hasRoot = true
	} else {
		cl.parent.Metrics.Increment("trace_send_no_root")
	}

	otelutil.AddSpanFields(span, map[string]interface{}{
		"kept":        shouldSend,
		"reason":      reason,
		"sampler":     key,
		"selector":    samplerSelector,
		"rate":        rate,
		"send_reason": sendReason,
		"hasRoot":     hasRoot,
	})
	cl.parent.Logger.Debug().WithField("key", key).Logf("making decision for trace")
	s = sendableTrace{
		Trace:           trace,
		reason:          reason,
		sampleKey:       key,
		samplerSelector: samplerSelector,
		rate:            rate,
		sendReason:      sendReason,
		shouldSend:      shouldSend,
	}

	return s, nil
}
