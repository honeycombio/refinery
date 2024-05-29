package gossip

import (
	"errors"
	"strings"
	"sync"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/redis"
	"golang.org/x/sync/errgroup"
)

const gossipRedisHealth = "gossip-redis"

var _ Gossiper = &GossipRedis{}

// GossipRedis is a Gossiper that uses Redis as the transport
// for gossip messages.
// It multiplexes messages to subscribers based on the channel
// name.
type GossipRedis struct {
	Redis  redis.Client    `inject:"redis"`
	Logger logger.Logger   `inject:""`
	Health health.Recorder `inject:""`
	eg     *errgroup.Group

	subscriptions map[Channel][]chan []byte
	channels      map[string]Channel
	lock          sync.RWMutex
	done          chan struct{}

	startstop.Stopper
}

// Start starts the GossipRedis instance
// It starts a pub/sub channel on the Redis client.
// Once a message is received, GossipRedis will invoke
// all callbacks registered for that channel.
func (g *GossipRedis) Start() error {
	g.eg = &errgroup.Group{}
	g.done = make(chan struct{})
	g.subscriptions = make(map[Channel][]chan []byte)
	// these are constant strings to make sure that everyone is using the same channel numbers
	// for the same channel names, with a fallback to support testing.
	// channels whose name starts with "test" will be assigned a channel ID based on the 5th character
	// of the name.
	// Any other channel name is invalid and will panic.
	// This are all to avoid collisions with the ASCII values of the known channels and make it possible
	// for tests to run in parallel.
	g.channels = map[string]Channel{
		ChannelKeep:   'k',
		ChannelDrop:   'd',
		ChannelStress: 's',
	}

	g.Health.Register(gossipRedisHealth, redis.HealthCheckPeriod*2)

	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil
			default:
				err := g.Redis.ListenPubSubChannels(nil, func(_ string, b []byte) {
					g.Health.Ready(gossipRedisHealth, true)

					msg := Message(b)
					g.lock.RLock()
					chans := g.subscriptions[msg.Channel()][:] // copy the slice to avoid holding the lock while sending
					g.lock.RUnlock()
					// now start goroutines to send the message to all subscribers in parallel
					// we don't want to block the Redis listener or other subscribers
					for i := range chans {
						ch := chans[i]
						g.eg.Go(func() error {
							ch <- msg.Data()
							return nil
						})
					}
				}, g.onHealthCheck, g.done, "refinery-gossip")
				if err != nil {
					g.Logger.Warn().Logf("Error listening to refinery-gossip channel: %v", err)
				}
			}
		}

	})

	return nil
}

func (g *GossipRedis) Stop() error {
	g.Health.Unregister(gossipRedisHealth)
	close(g.done)
	return g.eg.Wait()
}

// Subscribe returns a channel that will receive messages from the Gossip channel.
// The channel has a buffer of depth; if the buffer is full, messages will be dropped.
func (g *GossipRedis) Subscribe(channel Channel, depth int) chan []byte {
	select {
	case <-g.done:
		return nil
	default:
	}

	ch := make(chan []byte, depth)
	g.lock.Lock()
	defer g.lock.Unlock()
	g.subscriptions[channel] = append(g.subscriptions[channel], ch)

	return ch
}

// Publish sends a message to all subscribers of a given channel.
func (g *GossipRedis) Publish(channel Channel, value []byte) error {
	select {
	case <-g.done:
		return errors.New("gossip has been stopped")
	default:
	}

	conn := g.Redis.GetPubSubConn()
	defer conn.Close()

	msg := NewMessage(channel, value)

	return conn.Publish("refinery-gossip", msg.Bytes())
}

func (g *GossipRedis) GetChannel(name string) Channel {
	g.lock.Lock()
	defer g.lock.Unlock()
	if ch, ok := g.channels[name]; ok {
		return ch
	}

	var ch Channel
	if strings.HasPrefix(name, "test") {
		ch = Channel(name[4]) // yes, this will panic if you don't specify a 5th character
	} else {
		panic("unknown channel name!") // this is a programming error, not a runtime error
	}

	g.Logger.Warn().Logf("Creating test gossip channel %s with value %c.", name, ch)
	g.channels[name] = ch
	return ch
}

func (g *GossipRedis) onHealthCheck(data string) {
	g.Health.Ready(gossipRedisHealth, true)
}
