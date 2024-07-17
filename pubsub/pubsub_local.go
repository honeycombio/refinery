package pubsub

import (
	"context"
	"sync"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

// LocalPubSub is a PubSub implementation that uses local channels to send messages; it does
// not communicate with any external processes.
// subs are individual channels for each subscription
type LocalPubSub struct {
	Config  config.Config   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	topics  map[string][]*LocalSubscription
	mut     sync.RWMutex
}

// Ensure that LocalPubSub implements PubSub
var _ PubSub = (*LocalPubSub)(nil)

type LocalSubscription struct {
	ps    *LocalPubSub
	topic string
	cb    SubscriptionCallback
	mut   sync.RWMutex
}

// Ensure that LocalSubscription implements Subscription
var _ Subscription = (*LocalSubscription)(nil)

// Start initializes the LocalPubSub
func (ps *LocalPubSub) Start() error {
	ps.topics = make(map[string][]*LocalSubscription)
	ps.Metrics.Register("local_pubsub_published", "counter")
	ps.Metrics.Register("local_pubsub_received", "counter")
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
	for _, subs := range ps.topics {
		for i := range subs {
			subs[i].cb = nil
		}
	}
	ps.topics = make(map[string][]*LocalSubscription, 0)
}

func (ps *LocalPubSub) ensureTopic(topic string) {
	if _, ok := ps.topics[topic]; !ok {
		ps.topics[topic] = make([]*LocalSubscription, 0)
	}
}

func (ps *LocalPubSub) Publish(ctx context.Context, topic, message string) error {
	ps.mut.Lock()
	defer ps.mut.Unlock()
	ps.ensureTopic(topic)
	ps.Metrics.Count("local_pubsub_published", 1)
	ps.Metrics.Count("local_pubsub_received", len(ps.topics[topic]))
	for _, sub := range ps.topics[topic] {
		// don't wait around for slow consumers
		if sub.cb != nil {
			go sub.cb(ctx, message)
		}
	}
	return nil
}

func (ps *LocalPubSub) Subscribe(ctx context.Context, topic string, callback SubscriptionCallback) Subscription {
	ps.mut.Lock()
	ps.ensureTopic(topic)
	sub := &LocalSubscription{ps: ps, topic: topic, cb: callback}
	ps.topics[topic] = append(ps.topics[topic], sub)
	ps.mut.Unlock()
	return sub
}

func (s *LocalSubscription) Close() {
	s.ps.mut.RLock()
	for _, sub := range s.ps.topics[s.topic] {
		if sub == s {
			sub.mut.Lock()
			sub.cb = nil
			sub.mut.Unlock()
			return
		}
	}
	s.ps.mut.RUnlock()
}
