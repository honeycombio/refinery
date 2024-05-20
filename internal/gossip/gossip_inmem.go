package gossip

import (
	"errors"

	"golang.org/x/sync/errgroup"
)

// InMemoryGossip is a Gossiper that uses an in-memory channel
type InMemoryGossip struct {
	channel        chan []byte
	subscribers    map[string][]func(data []byte)
	subscribeChans map[string][]chan []byte

	done chan struct{}
	eg   *errgroup.Group
}

var _ Gossiper = &InMemoryGossip{}

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

func (g *InMemoryGossip) SubscribeChan(channel string, depth int) chan []byte {
	select {
	case <-g.done:
		return nil
	default:
	}

	ch := make(chan []byte, depth)
	g.subscribeChans[channel] = append(g.subscribeChans[channel], ch)

	return ch
}

func (g *InMemoryGossip) Start() error {
	g.channel = make(chan []byte, 10)
	g.eg = &errgroup.Group{}
	g.subscribers = make(map[string][]func(data []byte))
	g.done = make(chan struct{})

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
				for _, ch := range g.subscribeChans[msg.key] {
					select {
					case ch <- msg.data:
					default:
					}
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
