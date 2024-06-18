package pubsub

import (
	"context"
	"sync"

	"github.com/honeycombio/refinery/config"
	"golang.org/x/exp/maps"
)

// LocalPubSub is a PubSub implementation that uses local channels to send messages; it does
// not communicate with any external processes.
type LocalPubSub struct {
	Config *config.Config `inject:""`
	topics map[string]*LocalTopic
	mut    sync.RWMutex
}

type LocalTopic struct {
	topic       string
	pubChan     chan string
	subscribers []chan string
	done        chan struct{}
	mut         sync.RWMutex
	once        sync.Once
}

func (ps *LocalPubSub) Start() error {
	ps.topics = make(map[string]*LocalTopic)
	return nil
}

func (ps *LocalPubSub) Stop() error {
	ps.Close()
	return nil
}

// assert that LocalPubSub implements PubSub
var _ PubSub = (*LocalPubSub)(nil)

// when a topic is created, it is stored in the topics map and a goroutine is
// started to listen for messages on the topic; each message is sent to all
// subscribers to the topic.
func (ps *LocalPubSub) NewTopic(ctx context.Context, topic string) Topic {
	t := &LocalTopic{
		topic:       topic,
		pubChan:     make(chan string, 10),
		subscribers: make([]chan string, 0),
		done:        make(chan struct{}),
	}

	go func() {
		for {
			select {
			case msg := <-t.pubChan:
				t.mut.RLock()
				subscribers := t.subscribers
				t.mut.RUnlock()
				for _, sub := range subscribers {
					select {
					case sub <- msg:
					case <-t.done:
						return
					}
				}
			case <-t.done:
				close(t.pubChan)
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
func (ps *LocalPubSub) Close() {
	ps.mut.Lock()
	topics := maps.Values(ps.topics)
	ps.topics = make(map[string]*LocalTopic)
	ps.mut.Unlock()

	for _, t := range topics {
		t.Close()
	}
}

// Publish sends a message to all subscribers of the topic
func (t *LocalTopic) Publish(ctx context.Context, message string) error {
	t.pubChan <- message
	return nil
}

// Subscribe returns a channel that will receive all messages published to the topic
func (t *LocalTopic) Subscribe(ctx context.Context) <-chan string {
	ch := make(chan string)
	t.mut.Lock()
	t.subscribers = append(t.subscribers, ch)
	t.mut.Unlock()
	return ch
}

// Close shuts down the topic and unsubscribes all subscribers
func (t *LocalTopic) Close() {
	t.once.Do(func() {
		close(t.done)
	})
}
