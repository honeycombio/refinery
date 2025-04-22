package pubsub

import (
	"context"
	"fmt"
	"log"
	"sync"

	g "cloud.google.com/go/pubsub"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
)

type GoogleSubscription struct {
	topic string
	sub   *g.Subscription
	cb    SubscriptionCallback
	done  chan any
	once  sync.Once
}

func (s *GoogleSubscription) Close() {
	s.sub.Delete(context.Background())
	s.once.Do(func() {
		close(s.done)
	})
}

var _ Subscription = (*GoogleSubscription)(nil)

type GooglePubSub struct {
	Config  config.Config   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	client  *g.Client
	subs    map[string][]*GoogleSubscription
	topics  map[string]*g.Topic
	mut     sync.RWMutex
}

// Ensure that GooglePubSub implements PubSub
var _ PubSub = (*GooglePubSub)(nil)

func (ps *GooglePubSub) Start() error {
	ctx := context.Background()
	project := ps.Config.GetGooglePeerManagement().ProjectID
	if project == "" {
		credentials, err := google.FindDefaultCredentials(ctx, compute.ComputeScope)
		if err != nil {
			return err
		}
		project = credentials.ProjectID
	}
	client, err := g.NewClient(ctx, project)
	if err != nil {
		return fmt.Errorf("g.NewClient: %w", err)
	}
	ps.client = client
	return nil
}

func (ps *GooglePubSub) Stop() error {
	ps.Close()
	return nil
}

func (ps *GooglePubSub) getTopic(name string) *g.Topic {
	ps.mut.RLock()
	var t *g.Topic
	if topic, ok := ps.topics[name]; ok {
		t = topic
		ps.mut.RUnlock()
	} else {
		ps.mut.RUnlock()
		ps.mut.Lock()
		t = ps.client.Topic(name)
		ps.topics[name] = t
		ps.mut.Unlock()
	}
	return t
}

func (ps *GooglePubSub) Publish(ctx context.Context, key, message string) error {
	name := ps.Config.GetGooglePeerManagement().Topic
	t := ps.getTopic(name)
	t.Publish(ctx, &g.Message{Data: []byte(message)})
	return nil
}

func (ps *GooglePubSub) Subscribe(ctx context.Context, key string, callback SubscriptionCallback) Subscription {
	name := ps.Config.GetGooglePeerManagement().Topic
	t := ps.getTopic(name)
	sub, err := ps.client.CreateSubscription(ctx, ps.Config.GetRedisIdentifier(), g.SubscriptionConfig{
		Topic:  t,
		Filter: `attributes.topic="unknown"`,
	})

	ps.mut.Lock()
	ps.subs[name] = append(ps.subs[name], &GoogleSubscription{
		topic: name,
		sub:   sub,
		cb:    callback,
		done:  make(chan any),
	})
	ps.mut.Unlock()

	if err != nil {
		log.Fatalf("CreateSubscription: %w", err)
		return nil
	}

	sub.Receive(ctx, func(iCtx context.Context, m *g.Message) {
		callback(iCtx, string(m.Data))
		m.Ack()
	})
	return nil
}

func (ps *GooglePubSub) Close() {
	ps.mut.Lock()
	for _, subs := range ps.subs {
		for _, sub := range subs {
			sub.Close()
		}
	}
	ps.subs = nil
	for _, topic := range ps.topics {
		topic.Stop()
	}
	ps.topics = nil
	ps.mut.Unlock()
	ps.client.Close()
}
