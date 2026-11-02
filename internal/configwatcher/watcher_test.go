package configwatcher

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/pubsub"
	"go.opentelemetry.io/otel/trace/noop"
)

// stalledPubSub publishes like a Redis that does not answer: Publish returns
// only when its context ends.
type stalledPubSub struct {
	pubsub.PubSub
}

func (stalledPubSub) Publish(ctx context.Context, topic, message string) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestReloadCallbackReturnsWhenPublishStalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const peerTimeout = 100 * time.Millisecond
		cw := &ConfigWatcher{
			Config: &config.MockConfig{
				PeerTimeout:         peerTimeout,
				GetGeneralConfigVal: config.GeneralConfig{ConfigReloadInterval: config.Duration(time.Minute)},
			},
			Logger: &logger.NullLogger{},
			PubSub: stalledPubSub{},
			Tracer: noop.NewTracerProvider().Tracer("test"),
		}

		returned := make(chan struct{})
		go func() {
			cw.ReloadCallback("config-hash", "rules-hash")
			close(returned)
		}()

		time.Sleep(peerTimeout)
		synctest.Wait()

		select {
		case <-returned:
		default:
			t.Fatal("ReloadCallback is still waiting for Publish after the peer timeout")
		}
	})
}
