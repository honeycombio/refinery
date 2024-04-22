package gossip

import "github.com/facebookgo/startstop"

type Gossiper interface {
	// Gossip sends a message to all peers that are listening on the channel
	Publish(channel string, message Message) error

	// Register a handler for messages on a channel
	// The handler will be called for each message received on the channel
	Register(channel string, handler func(Message)) error

	startstop.Starter
	startstop.Stopper
}

var _ Gossiper = &NullGossip{}

type NullGossip struct{}

func (g *NullGossip) Publish(channel string, message Message) error        { return nil }
func (g *NullGossip) Register(channel string, handler func(Message)) error { return nil }
func (g *NullGossip) Start() error                                         { return nil }
func (g *NullGossip) Stop() error                                          { return nil }
