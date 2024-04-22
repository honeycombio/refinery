package gossip

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/redis"
	"golang.org/x/sync/errgroup"
)

type GossipRedis struct {
	Redis  redis.Client  `inject:"redis"`
	Logger logger.Logger `inject:"logger"`
	eg     *errgroup.Group

	done chan struct{}

	startstop.Stopper
}

func (g *GossipRedis) Start() error {
	g.eg = &errgroup.Group{}
	g.done = make(chan struct{})

	return nil
}

func (g *GossipRedis) Stop() error {
	close(g.done)
	return g.eg.Wait()
}

func (g *GossipRedis) Register(channel string, handler func(msg Message)) error {
	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil
			default:
				err := g.Redis.ListenPubSubChannels(nil, func(channel string, b []byte) {
					message, err := newMessageFromString(string(b))
					if err != nil {
						g.Logger.Debug().Logf("Error parsing message: %s", err)
						return
					}
					handler(message)
				}, g.done, channel)
				if err != nil {
					g.Logger.Debug().Logf("Error listening to channel %s: %s", channel, err)
				}
			}
		}

	})

	return nil
}

func (g *GossipRedis) Publish(channel string, message Message) error {
	conn := g.Redis.GetPubSubConn()
	defer conn.Close()

	return conn.Publish(channel, message)
}

type Message struct {
	Key   string
	Value interface{}
}

func (m Message) String() string {
	var value string
	switch v := m.Value.(type) {
	case string:
		value = v
	case []byte:
		value = string(v)
	case int:
		value = strconv.Itoa(v)
	case uint:
		value = strconv.Itoa(int(v))
	case float64:
		value = strconv.FormatFloat(v, 'f', -1, 64)
	default:
		value = fmt.Sprintf("%v", v)
	}
	return m.Key + "/" + value
}

func newMessageFromString(str string) (Message, error) {
	values := strings.Split(str, "/")
	if len(values) != 2 {
		return Message{}, fmt.Errorf("Received invalid stress_level message: %s", str)
	}

	return Message{
		Key:   values[0],
		Value: values[1],
	}, nil
}
