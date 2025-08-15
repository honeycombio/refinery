package collect

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/types"
)

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

	// Our local trace cache. This deliberatly does not have a mutex around it;
	// there is no way to safely access this directly from the outside.
	cache cache.Cache

	// For reporting cache size asynchronously.
	lastCacheSize atomic.Int64
}

// NewCollectLoop creates a new CollectLoop instance
func NewCollectLoop(
	id int,
	parent *InMemCollector,
	cacheCapacity int,
	incomingSize int,
	peerSize int,
) *CollectLoop {
	return &CollectLoop{
		ID:        id,
		parent:    parent,
		cache:     cache.NewInMemCache(cacheCapacity, parent.Metrics, parent.Logger),
		incoming:  make(chan *types.Span, incomingSize),
		fromPeer:  make(chan *types.Span, peerSize),
		sendEarly: make(chan sendEarly, 1),

		// Important that this be unbuffered, so the sender blocks until the
		// signal has been received.
		pause: make(chan chan struct{}),
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

	tickerDuration := cl.parent.Config.GetTracesConfig().GetSendTickerValue()
	ticker := cl.parent.Clock.NewTicker(tickerDuration)
	defer ticker.Stop()

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
			tr, err := cl.parent.makeDecision(ctx, t, TraceSendGotRoot)
			if err != nil {
				sendExpiredTraceSpan.End()
				continue
			}
			cl.parent.send(ctx, tr)
		} else {
			if spanLimit > 0 && t.DescendantCount() > spanLimit {
				tr, err := cl.parent.makeDecision(ctx, t, TraceSendSpanLimit)
				if err != nil {
					sendExpiredTraceSpan.End()
					continue
				}
				cl.parent.send(ctx, tr)
			} else {
				tr, err := cl.parent.makeDecision(ctx, t, TraceSendExpired)
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
		// only eject traces that belong to this peer or the trace is an orphan
		if _, ok := cl.parent.IsMyTrace(trace.ID()); !ok && !trace.IsOrphan(traceTimeout, cl.parent.Clock.Now()) {
			cl.parent.Logger.Debug().WithFields(map[string]interface{}{
				"trace_id": trace.ID(),
			}).Logf("cannot eject trace that does not belong to this peer")

			continue
		}

		t, err := cl.parent.makeDecision(ctx, trace, TraceSendEjectedMemsize)
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
