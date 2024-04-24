package gossip

import (
	"bytes"
	"errors"

	"github.com/facebookgo/startstop"
	"golang.org/x/sync/errgroup"
)

// Gossiper is an interface for broadcasting messages to all receivers
// subscribed to a channel
type Gossiper interface {
	// Publish sends a message to all peers listening on the channel
	Publish(channel string, value []byte) error

	// Subscribe listens for messages on the channel
	Subscribe(channel string, callback func(data []byte)) error

	startstop.Starter
	startstop.Stopper
}

var _ Gossiper = &InMemoryGossip{}

// InMemoryGossip is a Gossiper that uses an in-memory channel
type InMemoryGossip struct {
	channel     chan []byte
	subscribers map[string][]func(data []byte)

	done chan struct{}
	eg   *errgroup.Group
}

func (g *InMemoryGossip) Publish(channel string, value []byte) error {
	msg := message{
		key:  channel,
		data: value,
	}

	select {
	case <-g.done:
		return errors.New("gossip has been stopped")
	case g.channel <- msg.ToBytes():
	default:
	}
	return nil
}

func (g *InMemoryGossip) Subscribe(channel string, callback func(data []byte)) error {
	select {
	case <-g.done:
		return errors.New("gossip has been stopped")
	default:
	}

	g.subscribers[channel] = append(g.subscribers[channel], callback)
	return nil
}

func (g *InMemoryGossip) Start() error {
	g.channel = make(chan []byte, 10)

	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil
			case value := <-g.channel:
				msg := newMessageFromBytes(value)
				callbacks := g.subscribers[msg.key]

				for _, cb := range callbacks {
					cb(msg.data)
				}
			}
		}
	})

	return nil
}
func (g *InMemoryGossip) Stop() error {
	close(g.done)
	close(g.channel)
	return g.eg.Wait()
}

type message struct {
	key  string
	data []byte
}

func (m message) ToBytes() []byte {
	return append([]byte(m.key+":"), m.data...)
}

func newMessageFromBytes(b []byte) message {
	splits := bytes.SplitN(b, []byte(":"), 2)
	return message{
		key:  string(splits[0]),
		data: splits[1],
	}
}
