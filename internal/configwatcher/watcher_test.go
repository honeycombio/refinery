package configwatcher

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/pubsub/pubsubtest"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestReloadCallbackReturnsWhenPublishStalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const peerTimeout = 100 * time.Millisecond
		stalled := &pubsubtest.OutagePubSub{}
		stalled.Down()
		cw := &ConfigWatcher{
			Config: &config.MockConfig{
				PeerTimeout:         peerTimeout,
				GetGeneralConfigVal: config.GeneralConfig{ConfigReloadInterval: config.Duration(time.Minute)},
			},
			Logger: &logger.NullLogger{},
			PubSub: stalled,
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
