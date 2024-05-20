package gossip

import (
	"errors"
	"fmt"
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

	lock           sync.RWMutex
	subscribeChans map[string][]chan []byte
	done           chan struct{}

	startstop.Stopper
}

// Start starts the GossipRedis instance
// It starts a pub/sub channel on the Redis client.
// Once a message is received, GossipRedis will invoke
// all callbacks registered for that channel.
func (g *GossipRedis) Start() error {
	g.eg = &errgroup.Group{}
	g.done = make(chan struct{})
	g.subscribeChans = make(map[string][]chan []byte)

	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil
			default:
				err := g.Redis.ListenPubSubChannels(nil, func(channel string, b []byte) {
					msg := newMessageFromBytes(b)
					g.lock.RLock()
					chans := g.subscribeChans[msg.key]
					g.lock.RUnlock()
					// we never block on sending to a subscriber; if it's full, we drop the message
					for _, ch := range chans {
						select {
						case ch <- msg.data:
						default:
							fmt.Println("dropped message", msg.key, string(msg.data))
						}
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

// SubscribeChan returns a channel that will receive messages from the Gossip channel.
// The channel has a buffer of depth; if the buffer is full, messages will be dropped.
func (g *GossipRedis) SubscribeChan(channel string, depth int) chan []byte {
	select {
	case <-g.done:
		return nil
	default:
	}

	ch := make(chan []byte, depth)
	g.lock.Lock()
	defer g.lock.Unlock()
	g.subscribeChans[channel] = append(g.subscribeChans[channel], ch)

	return ch
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
