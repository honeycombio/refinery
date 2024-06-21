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

var types = []string{
	"goredis",
	"local",
}

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
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				sub := ps.Subscribe(ctx, "topic")
				ch := sub.Channel()
				for msg := range ch {
					require.Contains(t, msg, "message")
				}
				wg.Done()
			}()
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < 10; i++ {
					err := ps.Publish(ctx, "topic", fmt.Sprintf("message %d", i))
					require.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				ps.Close()
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
			topics := make([]string, topicCount)
			for i := 0; i < topicCount; i++ {
				topics[i] = fmt.Sprintf("topic%d", i)
			}
			time.Sleep(100 * time.Millisecond)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				time.Sleep(500 * time.Millisecond)
				for j := 0; j < topicCount; j++ {
					for i := 0; i < messageCount; i++ {
						// we want a different sum for each topic
						err := ps.Publish(ctx, topics[j], fmt.Sprintf("%d", (i+1)*(j+1)))
						require.NoError(t, err)
					}
				}
				time.Sleep(100 * time.Millisecond)
				ps.Close()
				wg.Done()
			}()
			mut := sync.Mutex{}
			totals := make([]int, topicCount)
			subs := make([]pubsub.Subscription, topicCount)
			for i := 0; i < topicCount; i++ {
				wg.Add(1)
				go func(ix int) {
					subs[ix] = ps.Subscribe(ctx, topics[ix])
					for msg := range subs[ix].Channel() {
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
			var count, total, tmin, tmax int64
			mut := sync.Mutex{}

			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
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

			go func() {
				sub := ps.Subscribe(ctx, "topic")
				for msg := range sub.Channel() {
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
					mut.Lock()
					count++
					mut.Unlock()
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

func BenchmarkPubSub(b *testing.B) {
	ctx := context.Background()
	for _, typ := range types {
		b.Run(typ, func(b *testing.B) {
			ps := newPubSub(typ)
			time.Sleep(100 * time.Millisecond)
			count := 0
			mut := sync.Mutex{}

			wg := sync.WaitGroup{}
			wg.Add(2)
			b.ResetTimer()
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < b.N; i++ {
					err := ps.Publish(ctx, "topic", fmt.Sprintf("message %d", i))
					require.NoError(b, err)
				}
				require.Eventually(b, func() bool {
					mut.Lock()
					defer mut.Unlock()
					return count == b.N
				}, 5*time.Second, 10*time.Millisecond)
				ps.Close()
				wg.Done()
			}()

			go func() {
				sub := ps.Subscribe(ctx, "topic")
				for range sub.Channel() {
					mut.Lock()
					count++
					mut.Unlock()
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, typ := range types {
		b.Run(typ, func(b *testing.B) {
			ps := newPubSub(typ)
			topics := make([]string, topicCount)
			for i := 0; i < topicCount; i++ {
				topics[i] = fmt.Sprintf("topic%d", i)
			}
			mut := sync.RWMutex{}
			count := 0
			counts := make([]int, topicCount)

			wg := sync.WaitGroup{}
			wg.Add(1)
			b.ResetTimer()
			go func() {
				time.Sleep(100 * time.Millisecond)
				for i := 0; i < b.N; i++ {
					err := ps.Publish(ctx, topics[i%topicCount], fmt.Sprintf("message %d", i))
					require.NoError(b, err)
				}
				require.Eventually(b, func() bool {
					mut.RLock()
					defer mut.RUnlock()
					return count == b.N
				}, 1*time.Second, 100*time.Millisecond)
				ps.Close()
				wg.Done()
			}()
			for i := 0; i < topicCount; i++ {
				wg.Add(1)
				go func(ix int) {
					sub := ps.Subscribe(ctx, topics[ix])
					for range sub.Channel() {
						mut.Lock()
						count++
						counts[ix]++
						mut.Unlock()
					}
					wg.Done()
				}(i)
			}
			wg.Wait()
			ps.Close()
		})
	}
}
