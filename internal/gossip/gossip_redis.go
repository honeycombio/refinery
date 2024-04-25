package gossip

import (
	"errors"
	"sync"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/redis"
	"golang.org/x/sync/errgroup"
)

var _ Gossiper = &GossipRedis{}

// GossipRedis is a Gossiper that uses Redis as the transport
// for gossip messages.
// It multiplexes messages to subscribers based on the channel
// name.
type GossipRedis struct {
	Redis  redis.Client  `inject:"redis"`
	Logger logger.Logger `inject:""`
	eg     *errgroup.Group

	lock        sync.RWMutex
	subscribers map[string][]func(data []byte)
	done        chan struct{}

	startstop.Stopper
}

// Start starts the GossipRedis instance
// It starts a pub/sub channel on the Redis client.
// Once a message is received, GossipRedis will invoke
// all callbacks registered for that channel.
func (g *GossipRedis) Start() error {
	g.eg = &errgroup.Group{}
	g.done = make(chan struct{})
	g.subscribers = make(map[string][]func(data []byte))

	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil
			default:
				err := g.Redis.ListenPubSubChannels(nil, func(channel string, b []byte) {
					msg := newMessageFromBytes(b)
					g.lock.RLock()
					callbacks := g.subscribers[msg.key]
					g.lock.RUnlock()
					for _, cb := range callbacks {
						cb(msg.data)
					}
				}, g.done, "refinery-gossip")
				if err != nil {
					g.Logger.Debug().Logf("Error listening to refinery-gossip channel: %v", err)
				}
			}
		}

	})

	return nil
}

func (g *GossipRedis) Stop() error {
	close(g.done)
	return g.eg.Wait()
}

// Subscribe registers a callback for a given channel.
func (g *GossipRedis) Subscribe(channel string, cb func(data []byte)) error {
	select {
	case <-g.done:
		return errors.New("gossip has been stopped")
	default:
	}

	g.lock.Lock()
	defer g.lock.Unlock()
	g.subscribers[channel] = append(g.subscribers[channel], cb)

	return nil

}

// Publish sends a message to all subscribers of a given channel.
func (g *GossipRedis) Publish(channel string, value []byte) error {
	select {
	case <-g.done:
		return errors.New("gossip has been stopped")
	default:
	}

	conn := g.Redis.GetPubSubConn()
	defer conn.Close()

	msg := message{
		key:  channel,
		data: value,
	}

	return conn.Publish("refinery-gossip", msg.ToBytes())
}
