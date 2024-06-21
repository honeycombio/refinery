package pubsub

import (
	"context"
	"sync"

	"github.com/honeycombio/refinery/config"
)

// LocalPubSub is a PubSub implementation that uses local channels to send messages; it does
// not communicate with any external processes.
type LocalPubSub struct {
	Config *config.Config `inject:""`
	subs   []*LocalSubscription
	topics map[string]chan string
	mut    sync.RWMutex
}

// Ensure that LocalPubSub implements PubSub
var _ PubSub = (*LocalPubSub)(nil)

type LocalSubscription struct {
	topic string
	ch    chan string
	done  chan struct{}
}

// Ensure that LocalSubscription implements Subscription
var _ Subscription = (*LocalSubscription)(nil)

// Start initializes the LocalPubSub
func (ps *LocalPubSub) Start() error {
	ps.subs = make([]*LocalSubscription, 0)
	ps.topics = make(map[string]chan string)
	return nil
}

// Stop shuts down the LocalPubSub
func (ps *LocalPubSub) Stop() error {
	ps.Close()
	return nil
}

func (ps *LocalPubSub) Close() {
	ps.mut.Lock()
	defer ps.mut.Unlock()
	for _, sub := range ps.subs {
		sub.Close()
	}
	ps.subs = nil
}

func (ps *LocalPubSub) ensureTopic(topic string) chan string {
	if _, ok := ps.topics[topic]; !ok {
		ps.topics[topic] = make(chan string, 100)
	}
	return ps.topics[topic]
}

func (ps *LocalPubSub) Publish(ctx context.Context, topic, message string) error {
	ps.mut.RLock()
	ch := ps.ensureTopic(topic)
	ps.mut.RUnlock()
	select {
	case ch <- message:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (ps *LocalPubSub) Subscribe(ctx context.Context, topic string) Subscription {
	ps.mut.Lock()
	defer ps.mut.Unlock()
	ch := ps.ensureTopic(topic)
	sub := &LocalSubscription{
		topic: topic,
		ch:    ch,
		done:  make(chan struct{}),
	}
	ps.subs = append(ps.subs, sub)
	go func() {
		for {
			select {
			case <-sub.done:
				close(ch)
				return
			case msg := <-ch:
				sub.ch <- msg
			}
		}
	}()
	return sub
}

func (s *LocalSubscription) Channel() <-chan string {
	return s.ch
}

func (s *LocalSubscription) Close() {
	close(s.done)
}
