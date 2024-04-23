package gossip

import (
	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/redis"
	"golang.org/x/sync/errgroup"
)

var _ Gossiper = &GossipRedis{}

type GossipRedis struct {
	Redis  redis.Client  `inject:"redis"`
	Logger logger.Logger `inject:"logger"`
	eg     *errgroup.Group

	done chan struct{}

	msg chan message

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

func (g *GossipRedis) Subscribe(channel ...string) error {
	g.msg = make(chan message, len(channel))
	g.eg.Go(func() error {
		for {
			select {
			case <-g.done:
				return nil
			default:
				err := g.Redis.ListenPubSubChannels(nil, func(channel string, b []byte) {
					select {
					case g.msg <- message{channel, b}:
					case <-g.done:
					default:
					}
				}, g.done, channel...)
				if err != nil {
					g.Logger.Debug().Logf("Error listening to channel %s: %s", channel, err)
				}
			}
		}

	})

	return nil
}

func (g *GossipRedis) Publish(channel string, message []byte) error {
	conn := g.Redis.GetPubSubConn()
	defer conn.Close()

	return conn.Publish(channel, message)
}

func (g *GossipRedis) Receive() (string, []byte) {
	select {
	case msg := <-g.msg:
		return msg.channel, msg.data
	case <-g.done:
		return "", nil
	default:
		return "", nil
	}
}

type message struct {
	channel string
	data    []byte
}
