package pubsub_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/honeycombio/refinery/pubsub"
	"github.com/stretchr/testify/require"
)

var types = []string{"goredis", "local"}

func newPubSub(typ string) pubsub.PubSub {
	var ps pubsub.PubSub
	switch typ {
	case "goredis":
		ps = &pubsub.GoRedisPubSub{}
	case "local":
		ps = &pubsub.LocalPubSub{}
	default:
		panic("unknown pubsub type")
	}
	ps.Start()
	return ps
}

func TestPubSubBasics(t *testing.T) {
	ctx := context.Background()
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			ps := newPubSub(typ)
			topic := ps.NewTopic(ctx, "name")
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < 10; i++ {
					err := topic.Publish(ctx, fmt.Sprintf("message %d", i))
					require.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				topic.Close()
				wg.Done()
			}()
			go func() {
				ch := topic.Subscribe(ctx)
				for msg := range ch {
					require.Contains(t, msg, "message")
				}
				wg.Done()
			}()
			wg.Wait()
			ps.Close()
		})
	}
}

func TestPubSubMultiTopic(t *testing.T) {
	const topicCount = 100
	const messageCount = 10
	const expectedTotal = 55 // sum of 1 to messageCount
	ctx := context.Background()
	for _, typ := range types {
		t.Run(typ, func(t *testing.T) {
			ps := newPubSub(typ)
			topics := make([]pubsub.Topic, topicCount)
			for i := 0; i < topicCount; i++ {
				topics[i] = ps.NewTopic(ctx, fmt.Sprintf("topic%d", i))
			}
			time.Sleep(100 * time.Millisecond)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				for j := 0; j < topicCount; j++ {
					for i := 0; i < messageCount; i++ {
						// we want a different sum for each topic
						err := topics[j].Publish(ctx, fmt.Sprintf("%d", (i+1)*(j+1)))
						require.NoError(t, err)
					}
				}
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < topicCount; i++ {
					topics[i].Close()
				}
				wg.Done()
			}()
			mut := sync.Mutex{}
			totals := make([]int, topicCount)
			for i := 0; i < topicCount; i++ {
				wg.Add(1)
				go func(ix int) {
					ch := topics[ix].Subscribe(ctx)
					for msg := range ch {
						n, _ := strconv.Atoi(msg)
						mut.Lock()
						totals[ix] += n
						mut.Unlock()
					}
					wg.Done()
				}(i)
			}
			wg.Wait()
			ps.Close()
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
			topic := ps.NewTopic(ctx, "name")
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				for i := 0; i < messageCount; i++ {
					err := topic.Publish(ctx, fmt.Sprintf("%d", time.Now().UnixNano()))
					require.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				topic.Close()
				wg.Done()
			}()
			var count, total, tmin, tmax int64
			go func() {
				ch := topic.Subscribe(ctx)
				for msg := range ch {
					sent, err := strconv.Atoi(msg)
					require.NoError(t, err)
					rcvd := time.Now().UnixNano()
					latency := rcvd - int64(sent)
					require.True(t, latency >= 0)
					total += latency
					if tmin == 0 || latency < tmin {
						tmin = latency
					}
					if latency > tmax {
						tmax = latency
					}
					count++
				}
				wg.Done()
			}()
			wg.Wait()
			ps.Close()
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

func BenchmarkPublish(b *testing.B) {
	ctx := context.Background()
	for _, typ := range types {
		b.Run(typ, func(b *testing.B) {
			ps := newPubSub(typ)
			topic := ps.NewTopic(ctx, "name")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := topic.Publish(ctx, "message")
				require.NoError(b, err)
			}
			topic.Close()
			ps.Close()
		})
	}
}

func BenchmarkPubSub(b *testing.B) {
	ctx := context.Background()
	for _, typ := range types {
		b.Run(typ, func(b *testing.B) {
			ps := newPubSub(typ)
			topic := ps.NewTopic(ctx, "name")
			time.Sleep(100 * time.Millisecond)
			wg := sync.WaitGroup{}
			wg.Add(2)
			b.ResetTimer()
			go func() {
				for i := 0; i < b.N; i++ {
					err := topic.Publish(ctx, fmt.Sprintf("message %d", i))
					require.NoError(b, err)
				}
				time.Sleep(1 * time.Millisecond)
				topic.Close()
				wg.Done()
			}()
			count := 0
			go func() {
				ch := topic.Subscribe(ctx)
				for range ch {
					count++
				}
				wg.Done()
			}()
			wg.Wait()
			ps.Close()
			require.Equal(b, b.N, count)
		})
	}
}

func BenchmarkPubSubMultiTopic(b *testing.B) {
	const topicCount = 10
	ctx := context.Background()
	for _, typ := range types {
		b.Run(typ, func(b *testing.B) {
			ps := newPubSub(typ)
			topics := make([]pubsub.Topic, topicCount)
			for i := 0; i < topicCount; i++ {
				topics[i] = ps.NewTopic(ctx, fmt.Sprintf("topic%d", i))
			}
			time.Sleep(100 * time.Millisecond)
			wg := sync.WaitGroup{}
			wg.Add(1)
			b.ResetTimer()
			go func() {
				for i := 0; i < b.N; i++ {
					err := topics[i%topicCount].Publish(ctx, fmt.Sprintf("message %d", i))
					require.NoError(b, err)
				}
				time.Sleep(1 * time.Millisecond)
				for i := 0; i < topicCount; i++ {
					topics[i].Close()
				}
				wg.Done()
			}()
			mut := sync.Mutex{}
			counts := make([]int, topicCount)
			for i := 0; i < topicCount; i++ {
				wg.Add(1)
				go func(ix int) {
					ch := topics[ix].Subscribe(ctx)
					for range ch {
						mut.Lock()
						counts[ix]++
						mut.Unlock()
					}
					wg.Done()
				}(i)
			}
			wg.Wait()
			ps.Close()
			count := 0
			for i := 0; i < topicCount; i++ {
				count += counts[i]
			}
			require.Equal(b, b.N, count)
		})
	}
}
