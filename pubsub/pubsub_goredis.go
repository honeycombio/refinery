package pubsub

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
	"golang.org/x/exp/maps"
)

// GoRedisPubSub is a PubSub implementation that uses Redis as the message broker
// and the go-redis library to interact with Redis.
type GoRedisPubSub struct {
	rdb    *redis.Client
	topics map[string]*GoRedisTopic
	mut    sync.RWMutex
}

type GoRedisTopic struct {
	topic       string
	rdb         *redis.Client // duplicating this avoids a lock
	redisSub    *redis.PubSub
	subscribers []chan string
	done        chan struct{}
	mut         sync.RWMutex
	closed      bool
}

func NewGoRedisPubSub() *GoRedisPubSub {

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return &GoRedisPubSub{
		rdb:    rdb,
		topics: make(map[string]*GoRedisTopic),
	}
}

// assert that GoRedisPubSub implements PubSub
var _ PubSub = (*GoRedisPubSub)(nil)

// when a topic is created, it is stored in the topics map and a goroutine is
// started to listen for messages on the topic; each message is sent to all
// subscribers to the topic.
func (ps *GoRedisPubSub) NewTopic(ctx context.Context, topic string) Topic {
	t := &GoRedisTopic{
		rdb:         ps.rdb,
		topic:       topic,
		redisSub:    ps.rdb.Subscribe(ctx, topic),
		subscribers: make([]chan string, 0),
		done:        make(chan struct{}),
		closed:      false,
	}

	go func() {
		for {
			select {
			case msg := <-t.redisSub.Channel():
				t.mut.RLock()
				subscribers := t.subscribers
				t.mut.RUnlock()
				for _, sub := range subscribers {
					select {
					case sub <- msg.Payload:
					case <-t.done:
						return
					}
				}
			case <-t.done:
				t.redisSub.Close()
				t.mut.RLock()
				subscribers := t.subscribers
				for _, sub := range subscribers {
					close(sub)
				}
				t.subscribers = nil
				t.mut.RUnlock()
				return
			}
		}
	}()

	ps.mut.Lock()
	ps.topics[topic] = t
	ps.mut.Unlock()
	return t
}

// Close shuts down all topics and the redis connection
func (ps *GoRedisPubSub) Close() {
	ps.mut.Lock()
	topics := maps.Values(ps.topics)
	ps.mut.Unlock()

	for _, t := range topics {
		t.Close()
	}
	ps.rdb.Close()
}

// Publish sends a message to all subscribers of the topic
func (t *GoRedisTopic) Publish(ctx context.Context, message string) error {
	err := t.rdb.Publish(ctx, t.topic, message).Err()
	if err != nil {
		return err
	}
	return nil
}

// Subscribe returns a channel that will receive all messages published to the topic
func (t *GoRedisTopic) Subscribe(ctx context.Context) <-chan string {
	ch := make(chan string)
	t.mut.Lock()
	t.subscribers = append(t.subscribers, ch)
	t.mut.Unlock()
	return ch
}

// Close shuts down the topic and unsubscribes all subscribers
func (t *GoRedisTopic) Close() {
	t.mut.Lock()
	defer t.mut.Unlock()
	if t.closed {
		return
	}
	close(t.done)
	t.closed = true
}
