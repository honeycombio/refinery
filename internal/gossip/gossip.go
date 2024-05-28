package gossip

import (
	"github.com/facebookgo/startstop"
)

const (
	ChannelKeep   = "keep"
	ChannelDrop   = "drop"
	ChannelStress = "stress_health"
	ChannelTest1  = "test1"
	ChannelTest2  = "test2"
)

// Gossiper is an interface for broadcasting messages to all receivers
// subscribed to a channel
type Gossiper interface {
	// Publish sends a message to all peers listening on the channel
	Publish(channel Channel, value []byte) error

	// Subscribe returns a Go channel that will receive messages from the Gossip channel
	// (Redis already called the thing we listen to a channel, so we have to live with that)
	// The channel has a buffer of depth; if the buffer is full, messages will be dropped.
	Subscribe(channel Channel, depth int) chan []byte

	// GetChannel returns a Channel for the given string
	GetChannel(string) Channel

	startstop.Starter
	startstop.Stopper
}

type Channel byte

type Message []byte

func NewMessage(channel Channel, data []byte) Message {
	return append([]byte{byte(channel)}, data...)
}

func (m Message) Channel() Channel {
	return Channel(m[0])
}

func (m Message) Data() []byte {
	return m[1:]
}

func (m Message) Bytes() []byte {
	return m
}
