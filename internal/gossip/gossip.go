package gossip

import "github.com/facebookgo/startstop"

type Gossiper interface {
	// Gossip sends a message to all peers that are listening on the channel
	Publish(channel string, message []byte) error

	// Register a handler for messages on a channel
	// The handler will be called for each message received on the channel
	Subscribe(channel string) error

	Receive() []byte

	startstop.Starter
	startstop.Stopper
}

var _ Gossiper = &NullGossip{}

type NullGossip struct{}

func (g *NullGossip) Publish(channel string, message []byte) error { return nil }
func (g *NullGossip) Subscribe(channel string) error               { return nil }
func (g *NullGossip) Receive() []byte                              { return nil }

func (g *NullGossip) Start() error { return nil }
func (g *NullGossip) Stop() error  { return nil }
