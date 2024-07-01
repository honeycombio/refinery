package pubsub

import (
	"context"
	"strings"
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
	client redis.UniversalClient
	subs   []*GoRedisSubscription
	mut    sync.RWMutex
}

// Ensure that GoRedisPubSub implements PubSub
var _ PubSub = (*GoRedisPubSub)(nil)

type GoRedisSubscription struct {
	topic  string
	pubsub *redis.PubSub
	cb     func(msg string)
	done   chan struct{}
	once   sync.Once
	mut    sync.RWMutex
}

// Ensure that GoRedisSubscription implements Subscription
var _ Subscription = (*GoRedisSubscription)(nil)

func (ps *GoRedisPubSub) Start() error {
	options := &redis.UniversalOptions{}
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

		// we may have multiple hosts, separated by commas, so split them up and
		// use them as the addrs for the client (if there are multiples, it will
		// create a cluster client)
		hosts := strings.Split(host, ",")
		options.Addrs = hosts
		options.Username = username
		options.Password = pw
		options.DB = ps.Config.GetRedisDatabase()
	}
	client := redis.NewUniversalClient(options)

	// if an authcode was provided, use it to authenticate the connection
	if authcode != "" {
		pipe := client.Pipeline()
		pipe.Auth(context.Background(), authcode)
		if _, err := pipe.Exec(context.Background()); err != nil {
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
	for _, sub := range ps.subs {
		sub.Close()
	}
	ps.subs = nil
	ps.mut.Unlock()
	ps.client.Close()
}

func (ps *GoRedisPubSub) Publish(ctx context.Context, topic, message string) error {
	return ps.client.Publish(ctx, topic, message).Err()
}

func (ps *GoRedisPubSub) Subscribe(ctx context.Context, topic string, callback func(string)) Subscription {
	sub := &GoRedisSubscription{
		topic:  topic,
		pubsub: ps.client.Subscribe(ctx, topic),
		cb:     callback,
		done:   make(chan struct{}),
	}
	ps.mut.Lock()
	ps.subs = append(ps.subs, sub)
	ps.mut.Unlock()
	go func() {
		redisch := sub.pubsub.Channel()
		for {
			select {
			case <-sub.done:
				sub.mut.Lock()
				sub.cb = nil
				sub.mut.Unlock()
				return
			case msg := <-redisch:
				if msg == nil {
					continue
				}
				sub.mut.RLock()
				if sub.cb != nil {
					go sub.cb(msg.Payload)
				}
				sub.mut.RUnlock()
			}
		}
	}()
	return sub
}

func (s *GoRedisSubscription) Close() {
	s.once.Do(func() {
		close(s.done)
	})
}
