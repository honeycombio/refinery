package gossip

import (
	"errors"
	"fmt"
	"sync"

	"github.com/honeycombio/refinery/logger"
	"golang.org/x/sync/errgroup"
)

// InMemoryGossip is a Gossiper that uses an in-memory channel
type InMemoryGossip struct {
	Logger        logger.Logger `inject:""`
	gossipCh      chan []byte
	subscriptions map[string][]chan []byte

	done chan struct{}
	mut  sync.RWMutex
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
	default:
	}

	select {
	case <-g.done:
		return errors.New("gossip has been stopped")

	case g.gossipCh <- msg.ToBytes():
	default:
		g.Logger.Warn().WithFields(map[string]interface{}{
			"channel": channel,
			"msg":     string(value),
		}).Logf("Unable to publish message")
	}
	return nil
}

func (g *InMemoryGossip) Subscribe(channel string, depth int) chan []byte {
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
	g.subscriptions = make(map[string][]chan []byte)
	g.done = make(chan struct{})

	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil
			default:
			}

			select {
			case <-g.done:
				return nil

			case value := <-g.gossipCh:
				msg := newMessageFromBytes(value)
				g.mut.RLock()
				for _, ch := range g.subscriptions[msg.key] {
					select {
					case ch <- msg.data:
					default:
						g.Logger.Warn().WithFields(map[string]interface{}{
							"channel": msg.key,
							"msg":     string(msg.data),
						}).Logf("Unable to forward message")
					}
				}
				g.mut.RUnlock()
			}
		}
	})

	return nil
}

func (g *InMemoryGossip) Stop() error {
	close(g.done)

	fmt.Println("in memory close")

	close(g.gossipCh)
	return g.eg.Wait()
}
