package collect

import (
	"context"
	"fmt"
	"time"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/types"
	"go.opentelemetry.io/otel/attribute"
)

func (i *InMemCollector) processSpan(ctx context.Context, sp *types.Span, source string) {
	ctx, span := otelutil.StartSpanWith(ctx, i.Tracer, "collector.processSpan",
		"trace_id", sp.TraceID,
		"source", source,
		"dataset", sp.Dataset)
	defer span.End()

	var (
		targetShard sharder.Shard
		isMyTrace   bool
	)
	// if trace locality is enabled, we should forward all spans to its correct peer
	if i.Config.GetCollectionConfig().TraceLocalityEnabled() {
		targetShard = i.Sharder.WhichShard(sp.TraceID)
		isMyTrace = true
		if !targetShard.Equals(i.Sharder.MyShard()) {
			sp.APIHost = targetShard.GetAddress()
			i.PeerTransmission.EnqueueSpan(sp)
			return
		}
	} else {
		targetShard, isMyTrace = i.IsMyTrace(sp.TraceID)
		// if the span is a decision span and the trace no longer belong to us, we should not forward it to the peer
		if !isMyTrace && sp.IsDecisionSpan() {
			return
		}

	}

	tcfg := i.Config.GetTracesConfig()

	trace := i.cache.Get(sp.TraceID)
	if trace == nil {
		// if the trace has already been sent, just pass along the span
		if sr, keptReason, found := i.sampleTraceCache.CheckSpan(sp); found {
			span.SetAttributes(attribute.String("disposition", "already_sent"))
			i.Metrics.Increment("trace_sent_cache_hit")
			// bump the count of records on this trace -- if the root span isn't
			// the last late span, then it won't be perfect, but it will be better than
			// having none at all
			i.dealWithSentTrace(ctx, sr, keptReason, sp)
			return
		}

		// if the span is sent for signaling expired traces,
		// we should not add it to the cache
		if sp.Data["meta.refinery.expired_trace"] != nil {
			return
		}

		// trace hasn't already been sent (or this span is really old); let's
		// create a new trace to hold it
		span.SetAttributes(attribute.Bool("create_new_trace", true))
		i.Metrics.Increment("trace_accepted")

		timeout := tcfg.GetTraceTimeout()
		if timeout == 0 {
			timeout = 60 * time.Second
		}

		now := i.Clock.Now()
		trace = &types.Trace{
			APIHost:          sp.APIHost,
			APIKey:           sp.APIKey,
			Dataset:          sp.Dataset,
			TraceID:          sp.TraceID,
			ArrivalTime:      now,
			SendBy:           now.Add(timeout),
			DeciderShardAddr: targetShard.GetAddress(),
		}
		trace.SetSampleRate(sp.SampleRate) // if it had a sample rate, we want to keep it
		// push this into the cache and if we eject an unsent trace, send it ASAP
		i.cache.Set(trace)
	}
	// if the trace we got back from the cache has already been sent, deal with the
	// span.
	if trace.Sent {
		if sr, reason, found := i.sampleTraceCache.CheckSpan(sp); found {
			span.SetAttributes(attribute.String("disposition", "already_sent"))
			i.Metrics.Increment("trace_sent_cache_hit")
			i.dealWithSentTrace(ctx, sr, reason, sp)
			return
		}
		// trace has already been sent, but this is not in the sent cache.
		// we will just use the default late span reason as the sent reason which is
		// set inside the dealWithSentTrace function
		i.dealWithSentTrace(ctx, cache.NewKeptTraceCacheEntry(trace), "", sp)
	}

	// if the span is sent for signaling expired traces,
	// we should not add it to the cache
	if sp.Data["meta.refinery.expired_trace"] != nil {
		return
	}

	// great! trace is live. add the span.
	trace.AddSpan(sp)
	span.SetAttributes(attribute.String("disposition", "live_trace"))

	var spanForwarded bool
	// if this trace doesn't belong to us and it's not in sent state, we should forward a decision span to its decider
	if !trace.Sent && !isMyTrace {
		i.Metrics.Increment(source + "_router_peer")
		i.Logger.Debug().
			WithString("peer", targetShard.GetAddress()).
			Logf("Sending span to peer")

		i.forwardSpanToPeer(ctx, sp, trace, targetShard)
		spanForwarded = true
	}

	// we may override these values in conditions below
	var markTraceForSending bool
	timeout := tcfg.GetSendDelay()
	if timeout == 0 {
		timeout = 2 * time.Second // a sensible default
	}

	// if this is a root span and its destination shard is the current refinery, say so and send the trace
	if sp.IsRoot && !spanForwarded {
		markTraceForSending = true
		trace.RootSpan = sp
	}

	// if the span count has exceeded our SpanLimit, send the trace immediately
	if tcfg.SpanLimit > 0 && uint(trace.DescendantCount()) > tcfg.SpanLimit {
		markTraceForSending = true
		timeout = 0 // don't use a timeout in this case; this is an "act fast" situation
	}

	// we should only mark a trace for sending if we are the destination shard
	if markTraceForSending && !spanForwarded {
		span.SetAttributes(attribute.String("disposition", "marked_for_sending"))
		trace.SendBy = i.Clock.Now().Add(timeout)
		i.cache.Set(trace)
	}
}

func (i *InMemCollector) ProcessSpanImmediately(sp *types.Span) (processed bool, keep bool) {
	ctx, span := otelutil.StartSpanWith(context.Background(), i.Tracer, "collector.ProcessSpanImmediately",
		"trace_id", sp.TraceID,
		"dataset", sp.Dataset)
	defer span.End()

	var rate uint
	record, reason, found := i.sampleTraceCache.CheckSpan(sp)
	if !found {
		ctx, srSpan := otelutil.StartSpanWith(ctx, i.Tracer, "collector.checkStressRelief",
			"trace_id", sp.TraceID)
		rate, keep, reason = i.StressRelief.GetSampleRate(sp.TraceID)
		srSpan.SetAttributes(
			"sample.rate", rate,
			"sample.keep", keep,
			"sample.reason", reason,
		)
		srSpan.End()

		now := i.Clock.Now()
		trace := &types.Trace{
			APIHost:     sp.APIHost,
			APIKey:      sp.APIKey,
			Dataset:     sp.Dataset,
			TraceID:     sp.TraceID,
			ArrivalTime: now,
			SendBy:      now,
		}
		trace.SetSampleRate(rate)
		i.sampleTraceCache.Record(trace, keep, reason)
	} else {
		keep = record.Keep
		rate = record.SampleRate
	}

	if keep {
		ctx, sendSpan := otelutil.StartSpan(ctx, i.Tracer, "collector.sendImmediately")
		if i.Config.GetAddHostMetadataToTrace() {
			sp.AddSpanMetadataAttrs("meta.refinery.local_hostname", i.hostname)
		}
		sp.SampleRate = rate
		sendErr := i.Transmission.EnqueueSpan(sp)
		if sendErr != nil {
			sendSpan.SetAttributes("error", true, "error.message", sendErr.Error())
		}
		sendSpan.End()
	}

	span.SetAttributes(
		"sample.found_in_cache", found,
		"sample.keep", keep,
		"sample.rate", rate,
		"sample.reason", reason,
	)
	return true, keep
}

func (i *InMemCollector) dealWithSentTrace(ctx context.Context, tr cache.TraceSentRecord, keptReason string, sp *types.Span) {
	ctx, span := otelutil.StartSpanMulti(ctx, i.Tracer, "collector.dealWithSentTrace", map[string]interface{}{
		"trace_id":           sp.TraceID,
		"sample.keep":        tr.Keep,
		"sample.rate":        tr.SampleRate,
		"sample.kept_reason": keptReason,
	})
	defer span.End()

	if tr.Keep {
		if i.Config.GetAddHostMetadataToTrace() {
			sp.AddSpanMetadataAttrs("meta.refinery.local_hostname", i.hostname)
		}
		// use the trace's sample rate for this span
		sp.SampleRate = tr.SampleRate
		err := i.Transmission.EnqueueSpan(sp)
		if err != nil {
			span.SetAttributes(
				attribute.Bool("error", true),
				attribute.String("error.message", err.Error()),
			)
			i.Logger.Error().WithField("error", err).Logf("failed to enqueue late span")
		}
		i.Metrics.Increment(TraceSendLateSpan)
	}
}

func (i *InMemCollector) determineSamplingDecision(ctx context.Context, trace *types.Trace) (uint, string) {
	ctx, span := otelutil.StartSpanMulti(ctx, i.Tracer, "collector.determineSampling", map[string]interface{}{
		"trace_id": trace.TraceID,
		"dataset":  trace.Dataset,
	})
	defer span.End()

	var sampler sample.Sampler
	samplerKey, isLegacy := trace.GetSamplerKey()
	span.SetAttributes(
		attribute.String("sampler.key", samplerKey),
		attribute.Bool("sampler.is_legacy", isLegacy),
	)

	i.samplerLock.RLock()
	sampler = i.datasetSamplers[samplerKey]
	i.samplerLock.RUnlock()

	if sampler == nil {
		span.SetAttributes(attribute.String("sampler.status", "creating_new"))
		// no sampler exists yet for this trace's dataset; create one
		samplerType := i.Config.GetSamplerType()
		i.Logger.Debug().WithFields(map[string]interface{}{
			"dataset":      trace.Dataset,
			"sampler_type": fmt.Sprintf("%T", samplerType),
		}).Logf("creating new sampler for dataset")

		sampler = samplerType.CreateSampler(trace.Dataset)
		i.samplerLock.Lock()
		i.datasetSamplers[samplerKey] = sampler
		i.samplerLock.Unlock()
	}

	rate := sampler.GetSampleRate(trace.TraceID)
	keep := trace.ShouldKeep(trace.TraceID, rate)
	span.SetAttributes(
		attribute.Int("sample.rate", int(rate)),
		attribute.Bool("sample.keep", keep),
	)

	return rate, sampler.GetSampleReason(trace.TraceID, rate)
}

func (i *InMemCollector) forwardSpanToPeer(ctx context.Context, sp *types.Span, trace *types.Trace, targetShard sharder.Shard) {
	ctx, span := otelutil.StartSpanMulti(ctx, i.Tracer, "collector.forwardSpan", map[string]interface{}{
		"trace_id":    sp.TraceID,
		"target_peer": targetShard.GetAddress(),
		"source":      "peer_forward",
		"dataset":     sp.Dataset,
	})
	defer span.End()

	i.Metrics.Increment("peer_router_forward")

	// Create decision span for forwarding
	dc := i.createDecisionSpan(sp, trace, targetShard)

	err := i.PeerTransmission.EnqueueEvent(dc)
	if err != nil {
		span.SetAttributes(
			attribute.Bool("error", true),
			attribute.String("error.message", err.Error()),
		)
		i.Logger.Error().WithFields(map[string]interface{}{
			"error":       err.Error(),
			"trace_id":    sp.TraceID,
			"target_peer": targetShard.GetAddress(),
		}).Logf("failed to forward span to peer")
		return
	}

	span.SetAttributes(attribute.Bool("forward.success", true))
}

func (i *InMemCollector) updateTraceCache(ctx context.Context, trace *types.Trace, sp *types.Span) {
	ctx, span := otelutil.StartSpanMulti(ctx, i.Tracer, "collector.updateCache", map[string]interface{}{
		"trace_id":               trace.TraceID,
		"trace.span_count":       trace.SpanCount(),
		"trace.span_event_count": trace.SpanEventCount(),
		"trace.span_link_count":  trace.SpanLinkCount(),
		"cache.operation":        "update",
	})
	defer span.End()

	// Check cache capacity before update
	if i.cache.Size() >= i.cache.Capacity() {
		span.SetAttributes(
			attribute.String("cache.status", "full"),
			attribute.Int("cache.size", i.cache.Size()),
			attribute.Int("cache.capacity", i.cache.Capacity()),
		)
		i.Metrics.Increment("trace_send_ejected_full")
		return
	}

	// Update the trace in cache
	oldTrace := i.cache.Set(trace)
	if oldTrace != nil {
		span.SetAttributes(
			attribute.Bool("cache.replaced_existing", true),
			attribute.Int("cache.old_trace_span_count", int(oldTrace.SpanCount())),
		)
	}

	span.SetAttributes(attribute.String("cache.status", "updated"))
}
