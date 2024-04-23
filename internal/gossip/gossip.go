package gossip

import "github.com/facebookgo/startstop"

type Gossiper interface {
	// Gossip sends a message to all peers that are listening on the channel
	Publish(channel string, message []byte) error

	// Subscribe listens for messages on the channel
	Subscribe(channel ...string) error

	Receive() (string, []byte)

	startstop.Starter
	startstop.Stopper
}

var _ Gossiper = &NullGossip{}

type NullGossip struct{}

func (g *NullGossip) Publish(channel string, message []byte) error { return nil }
func (g *NullGossip) Subscribe(channel ...string) error            { return nil }
func (g *NullGossip) Receive() (string, []byte)                    { return "", nil }

func (g *NullGossip) Start() error { return nil }
func (g *NullGossip) Stop() error  { return nil }
