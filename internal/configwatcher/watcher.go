package configwatcher

import (
	"context"
	"strings"

	"github.com/facebookgo/startstop"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/pubsub"
	"go.opentelemetry.io/otel/trace"
)

const ConfigPubsubTopic = "cfg_update"

// This exists in internal because it depends on both config and pubsub; it
// should be part of config, but it also needs to be able to use pubsub, which
// depends on config, which would create a circular dependency.
// So we have to create it after creating pubsub and let dependency injection work.

// ConfigWatcher listens to configuration changes and publishes notice of them.
// It avoids sending duplicate messages by comparing the hash of the configs.
type ConfigWatcher struct {
	Config  config.Config
	PubSub  pubsub.PubSub
	Tracer  trace.Tracer `inject:"tracer"`
	subscr  pubsub.Subscription
	lastmsg string
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

	message := cfgHash + ":" + rulesHash
	// don't republish if we got this message from the subscription
	if message != cw.lastmsg {
		cw.PubSub.Publish(ctx, ConfigPubsubTopic, message)
	}
}

// SubscriptionListener listens for messages on the config pubsub topic and reloads the config
// if a new set of hashes is received.
func (cw *ConfigWatcher) SubscriptionListener(ctx context.Context, msg string) {
	_, span := otelutil.StartSpanWith(ctx, cw.Tracer, "ConfigWatcher.SubscriptionListener", "message", msg)
	defer span.End()

	cw.lastmsg = msg

	parts := strings.Split(msg, ":")
	if len(parts) != 2 {
		return
	}
	newCfg, newRules := parts[0], parts[1]
	// we have been told about a new config, but do we already have it?
	currentCfg, currentRules := cw.Config.GetHashes()
	if newCfg == currentCfg && newRules == currentRules {
		// we already have this config, no need to reload
		otelutil.AddSpanField(span, "loading_config", false)
		return
	}
	// it's new, so reload
	otelutil.AddSpanField(span, "loading_config", true)
	cw.Config.Reload()
}

func (cw *ConfigWatcher) Start() error {
	cw.subscr = cw.PubSub.Subscribe(context.Background(), ConfigPubsubTopic, cw.SubscriptionListener)
	cw.Config.RegisterReloadCallback(cw.ReloadCallback)
	return nil
}

func (cw *ConfigWatcher) Stop() error {
	cw.subscr.Close()
	return nil
}
