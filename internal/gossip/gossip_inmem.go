package gossip

import (
	"errors"
	"sync"

	"github.com/honeycombio/refinery/logger"
	"golang.org/x/sync/errgroup"
)

// InMemoryGossip is a Gossiper that uses an in-memory channel
type InMemoryGossip struct {
	Logger        logger.Logger `inject:""`
	gossipCh      chan []byte
	subscriptions map[Channel][]chan []byte
	channels      map[string]Channel

	done chan struct{}
	mut  sync.RWMutex
	eg   *errgroup.Group
}

var _ Gossiper = &InMemoryGossip{}

func (g *InMemoryGossip) Publish(channel Channel, value []byte) error {
	msg := NewMessage(channel, value)
	select {
	case <-g.done:
		return errors.New("gossip has been stopped")
	case g.gossipCh <- msg.Bytes():
	}
	return nil
}

func (g *InMemoryGossip) Subscribe(channel Channel, depth int) chan []byte {
	select {
	case <-g.done:
		return nil
	default:
	}

	ch := make(chan []byte, depth)
	g.mut.Lock()
	g.subscriptions[channel] = append(g.subscriptions[channel], ch)
	g.mut.Unlock()

	return ch
}

func (g *InMemoryGossip) Start() error {
	g.gossipCh = make(chan []byte, 10)
	g.eg = &errgroup.Group{}
	g.subscriptions = make(map[Channel][]chan []byte)
	g.channels = make(map[string]Channel)
	g.done = make(chan struct{})

	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil

			case value := <-g.gossipCh:
				msg := Message(value)
				g.mut.RLock()
				chans := g.subscriptions[msg.Channel()][:] // copy the slice to avoid holding the lock while sending
				g.mut.RUnlock()
				// now start goroutines to send the message to all subscribers in parallel
				// we don't want to block the Redis listener or other subscribers
				for i := range chans {
					ch := chans[i]
					g.eg.Go(func() error {
						ch <- msg.Data()
						return nil
					})
				}
			}
		}
	})

	return nil
}

func (g *InMemoryGossip) Stop() error {
	close(g.done)
	close(g.gossipCh)
	return g.eg.Wait()
}

func (g *InMemoryGossip) GetChannel(name string) Channel {
	g.mut.Lock()
	defer g.mut.Unlock()
	if ch, ok := g.channels[name]; ok {
		return ch
	}
	ch := Channel(len(g.channels))
	g.channels[name] = ch
	return ch
}
