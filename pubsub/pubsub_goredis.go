package pubsub

import (
	"context"
	"sync"

	"github.com/honeycombio/refinery/config"
	"github.com/redis/go-redis/v9"
	"golang.org/x/exp/maps"
)

// Notes for the future: we implemented a Redis-based PubSub system using 3
// different libraries: go-redis, redigo, and rueidis. All three implementations
// perform similarly, but go-redis is definitely the easiest to use for PubSub.
// The rueidis library is probably the fastest for high-performance Redis use
// when you want Redis to be a database or cache, and it has some nice features
// like automatic pipelining, but it's pretty low-level and the documentation is
// poor. Redigo is feeling pretty old at this point.

// GoRedisPubSub is a PubSub implementation that uses Redis as the message broker
// and the go-redis library to interact with Redis.
type GoRedisPubSub struct {
	Config *config.Config `inject:""`
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
	once        sync.Once
}

func (ps *GoRedisPubSub) Start() error {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ps.rdb = rdb
	ps.topics = make(map[string]*GoRedisTopic)
	return nil
}

func (ps *GoRedisPubSub) Stop() error {
	ps.Close()
	return nil
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
	t.once.Do(func() {
		close(t.done)
	})
}
