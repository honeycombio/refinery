package pubsub

import (
	"context"
	"crypto/tls"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
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
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	Tracer  trace.Tracer    `inject:"tracer"`
	client  redis.UniversalClient
	subs    []*GoRedisSubscription
	mut     sync.RWMutex
}

// Ensure that GoRedisPubSub implements PubSub
var _ PubSub = (*GoRedisPubSub)(nil)

type GoRedisSubscription struct {
	topic  string
	pubsub *redis.PubSub
	cb     SubscriptionCallback
	done   chan struct{}
	once   sync.Once
}

// Ensure that GoRedisSubscription implements Subscription
var _ Subscription = (*GoRedisSubscription)(nil)

func (ps *GoRedisPubSub) Start() error {
	options := new(redis.UniversalOptions)
	var (
		authcode           string
		clusterModeEnabled bool
	)

	if ps.Config != nil {
		redisCfg := ps.Config.GetRedisPeerManagement()
		hosts := []string{redisCfg.Host}
		// if we have a cluster host, use that instead of the regular host
		if len(redisCfg.ClusterHosts) > 0 {
			ps.Logger.Info().Logf("ClusterHosts was specified, setting up Redis Cluster")
			hosts = redisCfg.ClusterHosts
			clusterModeEnabled = true
		}

		authcode = redisCfg.AuthCode

		options.Addrs = hosts
		options.Username = redisCfg.Username
		options.Password = redisCfg.Password
		options.DB = redisCfg.Database

		if redisCfg.UseTLS {
			ps.Logger.Info().WithField("TLSInsecure", redisCfg.UseTLSInsecure).Logf("Using TLS with Redis")
			options.TLSConfig = &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: redisCfg.UseTLSInsecure,
			}
		}
	}

	var client redis.UniversalClient
	if clusterModeEnabled {
		ps.Logger.Info().WithField("hosts", options.Addrs).Logf("Using Redis Cluster Client")
		client = redis.NewClusterClient(options.Cluster())
	} else {
		ps.Logger.Info().WithField("hosts", options.Addrs).Logf("Using Redis Universal client")
		client = redis.NewUniversalClient(options)
	}

	// if an authcode was provided, use it to authenticate the connection
	if authcode != "" {
		ps.Logger.Info().Logf("Using Redis AuthCode to authenticate connection")
		pipe := client.Pipeline()
		pipe.Auth(context.Background(), authcode)
		if _, err := pipe.Exec(context.Background()); err != nil {
			return err
		}
	}

	ps.Metrics.Register("redis_pubsub_published", "counter")
	ps.Metrics.Register("redis_pubsub_received", "counter")

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
	ctx, span := otelutil.StartSpanMulti(ctx, ps.Tracer, "GoRedisPubSub.Publish", map[string]interface{}{
		"topic":   topic,
		"message": message,
	})

	defer span.End()

	ps.Metrics.Count("redis_pubsub_published", 1)
	return ps.client.Publish(ctx, topic, message).Err()
}

// Subscribe creates a new Subscription to the given topic, and calls the provided callback
// whenever a message is received on that topic.
// Note that the same topic is Subscribed to multiple times, this will incur a separate
// connection to Redis for each Subscription.
func (ps *GoRedisPubSub) Subscribe(ctx context.Context, topic string, callback SubscriptionCallback) Subscription {
	ctx, span := otelutil.StartSpanWith(ctx, ps.Tracer, "GoRedisPubSub.Subscribe", "topic", topic)
	defer span.End()

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
		receiveRootCtx := context.Background()
		redisch := sub.pubsub.Channel()
		for {
			select {
			case <-sub.done:
				return
			case msg := <-redisch:
				if msg == nil {
					continue
				}
				receiveCtx, span := otelutil.StartSpanMulti(receiveRootCtx, ps.Tracer, "GoRedisPubSub.Receive", map[string]interface{}{
					"topic":              topic,
					"message_queue_size": len(redisch),
					"message":            msg.Payload,
				})
				ps.Metrics.Count("redis_pubsub_received", 1)

				go func(cbCtx context.Context, span trace.Span, payload string) {
					defer span.End()

					sub.cb(cbCtx, payload)
				}(receiveCtx, span, msg.Payload)
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
