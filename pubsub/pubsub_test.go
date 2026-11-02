package pubsub_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/honeycombio/refinery/config"
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
			Config: &config.MockConfig{
				GetRedisPeerManagementVal: config.RedisPeerManagementConfig{
					ClusterName: "test",
				},
			},
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

			topic := ps.FormatTopic("topic")
			ps.Subscribe(ctx, topic, l1.Listen)

			if typ == "goredis" {
				assert.Contains(t, topic, "test")
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < 10; i++ {
					err := ps.Publish(ctx, topic, fmt.Sprintf("message %d", i))
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
			topic := ps.FormatTopic("topic")
			ps.Subscribe(ctx, topic, l1.Listen)
			ps.Subscribe(ctx, topic, l2.Listen)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < messageCount; i++ {
					err := ps.Publish(ctx, topic, fmt.Sprintf("message %d", i))
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
				topic := ps.FormatTopic(fmt.Sprintf("topic%d", i))
				topics[i] = topic
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
			topic := ps.FormatTopic("topic")
			go func() {
				time.Sleep(300 * time.Millisecond)
				for i := 0; i < messageCount; i++ {
					err := ps.Publish(ctx, topic, fmt.Sprintf("%d", time.Now().UnixNano()))
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

			ps.Subscribe(ctx, topic, func(ctx context.Context, msg string) {
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
			topic := ps.FormatTopic("topic")
			ps.Subscribe(ctx, topic, li.Listen)

			wg := sync.WaitGroup{}
			wg.Add(1)
			b.ResetTimer()
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < b.N; i++ {
					err := ps.Publish(ctx, topic, fmt.Sprintf("message %d", i))
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

// hungRedis stands in for a Redis that stops answering. While hung it accepts
// connections and says nothing, so a client waits until it gives up. Once it
// is told to forward, it connects clients to the Redis at target.
type hungRedis struct {
	ln      net.Listener
	target  string
	forward atomic.Bool
	mut     sync.Mutex
	conns   []net.Conn
}

func newHungRedis(t *testing.T, target string) *hungRedis {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	h := &hungRedis{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			h.mut.Lock()
			h.conns = append(h.conns, c)
			h.mut.Unlock()
			if h.forward.Load() {
				go h.pipe(c)
			}
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		h.mut.Lock()
		defer h.mut.Unlock()
		for _, c := range h.conns {
			c.Close()
		}
	})
	return h
}

func (h *hungRedis) addr() string { return h.ln.Addr().String() }

func (h *hungRedis) accepted() int {
	h.mut.Lock()
	defer h.mut.Unlock()
	return len(h.conns)
}

func (h *hungRedis) pipe(client net.Conn) {
	upstream, err := net.Dial("tcp", h.target)
	if err != nil {
		client.Close()
		return
	}
	h.mut.Lock()
	h.conns = append(h.conns, upstream)
	h.mut.Unlock()
	go io.Copy(upstream, client)
	io.Copy(client, upstream)
}

// TestGoRedisPubSubSubscribeDoesNotWaitForRedis tests that Subscribe returns
// while Redis does not answer, and that the subscription delivers messages
// once Redis does. A host must be able to start while Redis is down.
func TestGoRedisPubSubSubscribeDoesNotWaitForRedis(t *testing.T) {
	redis := newHungRedis(t, "localhost:6379")

	m := &metrics.NullMetrics{}
	m.Start()
	ps := &pubsub.GoRedisPubSub{
		Config: &config.MockConfig{
			GetRedisPeerManagementVal: config.RedisPeerManagementConfig{
				Host:        redis.addr(),
				ClusterName: "hung-test",
			},
		},
		Metrics: m,
		Tracer:  noop.NewTracerProvider().Tracer("test"),
		Logger:  &logger.NullLogger{},
	}
	require.NoError(t, ps.Start())
	t.Cleanup(ps.Close)

	// go-redis waits 5 seconds for a connection that never answers. Subscribe
	// must not wait for it.
	listener := &pubsubListener{}
	topic := ps.FormatTopic("topic")
	returned := make(chan struct{})
	go func() {
		ps.Subscribe(context.Background(), topic, listener.Listen)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Subscribe waited for a Redis that does not answer")
	}

	// Keep Redis silent until go-redis has given up on its first attempt, so
	// the subscription has to recover from a failed SUBSCRIBE.
	require.Eventually(t, func() bool { return redis.accepted() > 0 },
		time.Second, 10*time.Millisecond, "go-redis never tried to connect")
	time.Sleep(6 * time.Second)

	// Redis answers again. The subscription must receive what is published.
	redis.forward.Store(true)
	require.Eventually(t, func() bool {
		_ = ps.Publish(context.Background(), topic, "hello")
		return len(listener.Messages()) > 0
	}, 20*time.Second, 200*time.Millisecond, "the subscription never recovered")
}
