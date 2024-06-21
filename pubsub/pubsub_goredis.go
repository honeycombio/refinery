package pubsub

import (
	"context"
	"sync"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/redis/go-redis/v9"
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
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`
	client *redis.Client
	subs   []*GoRedisSubscription
	mut    sync.RWMutex
}

// Ensure that GoRedisPubSub implements PubSub
var _ PubSub = (*GoRedisPubSub)(nil)

type GoRedisSubscription struct {
	topic  string
	pubsub *redis.PubSub
	ch     chan string
	done   chan struct{}
	once   sync.Once
}

// Ensure that GoRedisSubscription implements Subscription
var _ Subscription = (*GoRedisSubscription)(nil)

func (ps *GoRedisPubSub) Start() error {
	options := &redis.Options{}
	authcode := ""

	if ps.Config != nil {
		host, err := ps.Config.GetRedisHost()
		if err != nil {
			return err
		}
		username, err := ps.Config.GetRedisUsername()
		if err != nil {
			return err
		}
		pw, err := ps.Config.GetRedisPassword()
		if err != nil {
			return err
		}

		authcode, err = ps.Config.GetRedisAuthCode()
		if err != nil {
			return err
		}

		options.Addr = host
		options.Username = username
		options.Password = pw
		options.DB = ps.Config.GetRedisDatabase()
	}
	client := redis.NewClient(options)

	// if an authcode was provided, use it to authenticate the connection
	if authcode != "" {
		if err := client.Conn().Auth(context.Background(), authcode).Err(); err != nil {
			return err
		}
	}

	ps.client = client
	ps.subs = make([]*GoRedisSubscription, 0)
	return nil
}

func (ps *GoRedisPubSub) Stop() error {
	ps.Close()
	return nil
}

func (ps *GoRedisPubSub) Close() {
	ps.mut.Lock()
	defer ps.mut.Unlock()
	for _, sub := range ps.subs {
		sub.Close()
	}
	ps.subs = nil
	ps.client.Close()
}

func (ps *GoRedisPubSub) Publish(ctx context.Context, topic, message string) error {
	ps.mut.RLock()
	defer ps.mut.RUnlock()
	return ps.client.Publish(ctx, topic, message).Err()
}

func (ps *GoRedisPubSub) Subscribe(ctx context.Context, topic string) Subscription {
	ps.mut.Lock()
	defer ps.mut.Unlock()
	sub := &GoRedisSubscription{
		topic:  topic,
		pubsub: ps.client.Subscribe(ctx, topic),
		ch:     make(chan string, 100),
		done:   make(chan struct{}),
	}
	ps.subs = append(ps.subs, sub)
	go func() {
		redisch := sub.pubsub.Channel()
		for {
			select {
			case <-sub.done:
				close(sub.ch)
				return
			case msg := <-redisch:
				if msg == nil {
					continue
				}
				select {
				case sub.ch <- msg.Payload:
				default:
					ps.Logger.Warn().WithField("topic", topic).Logf("Dropping subscription message because channel is full")
				}
			}
		}
	}()
	return sub
}

func (s *GoRedisSubscription) Channel() <-chan string {
	return s.ch
}

func (s *GoRedisSubscription) Close() {
	s.once.Do(func() {
		s.pubsub.Close()
		close(s.done)
	})
}
