package gossip

import (
	"bytes"

	"github.com/facebookgo/startstop"
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
