package configwatcher

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const ConfigPubsubTopic = "cfg_update"

// This exists in internal because it depends on both config and pubsub.
// So we have to create it after creating pubsub and let dependency injection work.

// ConfigWatcher listens for configuration changes and publishes notice of them.
// It avoids sending duplicate messages by comparing the hash of the configs.
type ConfigWatcher struct {
	Config  config.Config   `inject:""`
	PubSub  pubsub.PubSub   `inject:""`
	Tracer  trace.Tracer    `inject:"tracer"`
	Clock   clockwork.Clock `inject:""`
	subscr  pubsub.Subscription
	msgTime time.Time
	done    chan struct{}
	mut     sync.RWMutex
	startstop.Starter
	startstop.Stopper
}

// ReloadCallback is used to tell others that the config has changed.
// This gets called whenever it has actually changed, but it might have
// changed because we were told about it in pubsub, so we don't publish
// a message if the hashes are the same.
func (cw *ConfigWatcher) ReloadCallback(cfgHash, rulesHash string) {
	ctx := context.Background()
	ctx, span := otelutil.StartSpanMulti(ctx, cw.Tracer, "ConfigWatcher.ReloadCallback", map[string]any{
		"new_config_hash": cfgHash,
		"new_rules_hash":  rulesHash,
	})
	defer span.End()

	// don't publish if we have recently received a message (this avoids storms)
	now := time.Now()
	cw.mut.RLock()
	msgTime := cw.msgTime
	cw.mut.RUnlock()
	if now.Sub(msgTime) < time.Duration(cw.Config.GetGeneralConfig().ConfigReloadInterval) {
		otelutil.AddSpanField(span, "sending", false)
		return
	}

	message := now.Format(time.RFC3339)
	otelutil.AddSpanFields(span, map[string]any{"sending": true, "message": message})
	cw.PubSub.Publish(ctx, ConfigPubsubTopic, message)
}

// SubscriptionListener listens for messages on the config pubsub topic and reloads the config
// if a new set of hashes is received.
func (cw *ConfigWatcher) SubscriptionListener(ctx context.Context, msg string) {
	_, span := otelutil.StartSpanWith(ctx, cw.Tracer, "ConfigWatcher.SubscriptionListener", "message", msg)
	defer span.End()

	// parse message as a time in RFC3339 format
	msgTime, err := time.Parse(time.RFC3339, msg)
	if err != nil {
		return
	}
	cw.mut.Lock()
	cw.msgTime = msgTime
	cw.mut.Unlock()
	// maybe reload the config (it will only reload if the hashes are different,
	// and if they were, it will call the ReloadCallback)
	cw.Config.Reload()
}

// Monitor periodically wakes up and tells the config to reload itself.
// If it changed, it will publish a message to the pubsub through the ReloadCallback.
func (cw *ConfigWatcher) monitor() {
	cw.done = make(chan struct{})
	cfgReload := cw.Config.GetGeneralConfig().ConfigReloadInterval
	// adjust the requested time by +/- 10% to avoid everyone reloading at the same time
	reload := time.Duration(float64(cfgReload) * (0.9 + 0.2*rand.Float64()))
	ticker := time.NewTicker(time.Duration(reload))
	for {
		select {
		case <-cw.done:
			return
		case <-ticker.C:
			cw.Config.Reload()
		}
	}
}

func (cw *ConfigWatcher) Start() error {
	if cw.Tracer == nil {
		cw.Tracer = noop.NewTracerProvider().Tracer("test")
	}
	if cw.Config.GetGeneralConfig().ConfigReloadInterval != 0 {
		go cw.monitor()
	}
	cw.subscr = cw.PubSub.Subscribe(context.Background(), ConfigPubsubTopic, cw.SubscriptionListener)
	cw.Config.RegisterReloadCallback(cw.ReloadCallback)
	return nil
}

func (cw *ConfigWatcher) Stop() error {
	close(cw.done)
	cw.subscr.Close()
	return nil
}
