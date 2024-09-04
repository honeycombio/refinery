package pubsub_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

var types = []string{
	"goredis",
	"local",
}

func newPubSub(typ string) pubsub.PubSub {
	var ps pubsub.PubSub
	m := &metrics.NullMetrics{}
	m.Start()
	tracer := noop.NewTracerProvider().Tracer("test")
	switch typ {
	case "goredis":
		ps = &pubsub.GoRedisPubSub{
			Metrics: m,
			Tracer:  tracer,
			Logger:  &logger.NullLogger{},
		}
	case "local":
		ps = &pubsub.LocalPubSub{
			Metrics: m,
		}
	default:
		panic("unknown pubsub type")
	}
	ps.Start()
	return ps
}

type pubsubListener struct {
	lock sync.Mutex
	msgs []string
}

func (l *pubsubListener) Listen(ctx context.Context, msg string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.msgs = append(l.msgs, msg)
}

func (l *pubsubListener) Messages() []string {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.msgs
}

func TestPubSubBasics(t *testing.T) {
	ctx := context.Background()
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			ps := newPubSub(typ)

			l1 := &pubsubListener{}
			ps.Subscribe(ctx, "topic", l1.Listen)

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < 10; i++ {
					err := ps.Publish(ctx, "topic", fmt.Sprintf("message %d", i))
					assert.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				wg.Done()
			}()
			wg.Wait()
			ps.Close()
			require.Len(t, l1.Messages(), 10)
		})
	}
}

func TestPubSubMultiSubscriber(t *testing.T) {
	const messageCount = 10
	ctx := context.Background()
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			ps := newPubSub(typ)
			l1 := &pubsubListener{}
			l2 := &pubsubListener{}
			ps.Subscribe(ctx, "topic", l1.Listen)
			ps.Subscribe(ctx, "topic", l2.Listen)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < messageCount; i++ {
					err := ps.Publish(ctx, "topic", fmt.Sprintf("message %d", i))
					require.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				wg.Done()
			}()
			wg.Wait()
			ps.Close()
			require.Len(t, l1.Messages(), messageCount)
			require.Len(t, l2.Messages(), messageCount)
		})
	}
}

func TestPubSubMultiTopic(t *testing.T) {
	const topicCount = 3
	const messageCount = 10
	const expectedTotal = 55 // sum of [1..messageCount]
	ctx := context.Background()
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			ps := newPubSub(typ)
			time.Sleep(500 * time.Millisecond)
			topics := make([]string, topicCount)
			listeners := make([]*pubsubListener, topicCount)
			for i := 0; i < topicCount; i++ {
				topics[i] = fmt.Sprintf("topic%d", i)
				listeners[i] = &pubsubListener{}
			}
			totals := make([]int, topicCount)
			subs := make([]pubsub.Subscription, topicCount)
			for ix := 0; ix < topicCount; ix++ {
				subs[ix] = ps.Subscribe(ctx, topics[ix], listeners[ix].Listen)
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				time.Sleep(100 * time.Millisecond)
				for j := 0; j < topicCount; j++ {
					for i := 0; i < messageCount; i++ {
						// we want a different sum for each topic
						err := ps.Publish(ctx, topics[j], fmt.Sprintf("%d", (i+1)*(j+1)))
						require.NoError(t, err)
					}
				}
				time.Sleep(500 * time.Millisecond)
				ps.Close()
				wg.Done()
			}()
			wg.Wait()
			for ix := 0; ix < topicCount; ix++ {
				assert.Len(t, listeners[ix].Messages(), messageCount, "topic %d", ix)
				for _, msg := range listeners[ix].Messages() {
					n, _ := strconv.Atoi(msg)
					totals[ix] += n
				}
			}

			// validate that all the topics each add up to the desired total
			for i := 0; i < topicCount; i++ {
				require.Equal(t, expectedTotal*(i+1), totals[i])
			}
		})
	}
}

func TestPubSubLatency(t *testing.T) {
	const messageCount = 1000
	ctx := context.Background()
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			ps := newPubSub(typ)
			var count, total, tmin, tmax int64
			mut := sync.Mutex{}

			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				time.Sleep(300 * time.Millisecond)
				for i := 0; i < messageCount; i++ {
					err := ps.Publish(ctx, "topic", fmt.Sprintf("%d", time.Now().UnixNano()))
					require.NoError(t, err)
				}

				// now wait for all messages to arrive
				require.Eventually(t, func() bool {
					mut.Lock()
					done := count == messageCount
					mut.Unlock()
					return done
				}, 5*time.Second, 100*time.Millisecond)

				ps.Close()
				wg.Done()
			}()

			ps.Subscribe(ctx, "topic", func(ctx context.Context, msg string) {
				sent, err := strconv.Atoi(msg)
				require.NoError(t, err)
				rcvd := time.Now().UnixNano()
				latency := rcvd - int64(sent)
				require.True(t, latency >= 0)
				mut.Lock()
				total += latency
				if tmin == 0 || latency < tmin {
					tmin = latency
				}
				if latency > tmax {
					tmax = latency
				}
				count++
				mut.Unlock()
			})
			wg.Done()

			wg.Wait()
			require.Equal(t, int64(messageCount), count)
			require.True(t, total > 0)
			average := total / int64(count)
			t.Logf("average: %d ns, min: %d ns, max: %d ns", average, tmin, tmax)
			// in general, we want low latency, so we put some ballpark numbers here
			// to make sure we're not doing something crazy
			require.Less(t, average, int64(100*time.Millisecond))
			require.Less(t, tmax, int64(500*time.Millisecond))
		})
	}
}

func BenchmarkPubSub(b *testing.B) {
	ctx := context.Background()
	for _, typ := range types {
		b.Run(typ, func(b *testing.B) {
			ps := newPubSub(typ)
			time.Sleep(100 * time.Millisecond)

			li := &pubsubListener{}
			ps.Subscribe(ctx, "topic", li.Listen)

			wg := sync.WaitGroup{}
			wg.Add(1)
			b.ResetTimer()
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < b.N; i++ {
					err := ps.Publish(ctx, "topic", fmt.Sprintf("message %d", i))
					require.NoError(b, err)
				}
				require.EventuallyWithT(b, func(collect *assert.CollectT) {
					assert.Len(collect, li.Messages(), b.N)
				}, 5*time.Second, 10*time.Millisecond)
				ps.Close()
				wg.Done()
			}()

			wg.Wait()
			require.Len(b, li.Messages(), b.N)
		})
	}
}
