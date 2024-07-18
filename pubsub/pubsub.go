package pubsub

import (
	"context"

	"github.com/facebookgo/startstop"
)

// general usage:
// pubsub := pubsub.NewXXXPubSub()
// pubsub.Start()
// defer pubsub.Stop()
// ctx := context.Background()
// sub := pubsub.Subscribe(ctx, "topic", func(msg string) {
// 	fmt.Println(msg)
// }
// pubsub.Publish(ctx, "topic", "message")
// sub.Close()  // optional
// pubsub.Close()

type PubSub interface {
	// Publish sends a message to all subscribers of the specified topic.
	Publish(ctx context.Context, topic, message string) error
	// Subscribe returns a Subscription to the specified topic.
	// The callback will be called for each message published to the topic.
	// There is no unsubscribe method; close the subscription to stop receiving messages.
	// The subscription only exists to provide a way to stop receiving messages; if you don't need to stop,
	// you can ignore the return value.
	Subscribe(ctx context.Context, topic string, callback SubscriptionCallback) Subscription
	// Close shuts down all topics and the pubsub connection.
	Close()

	// we want to embed startstop.Starter and startstop.Stopper so that we
	// can participate in injection
	startstop.Starter
	startstop.Stopper
}

// SubscriptionCallback is the function signature for a subscription callback.
type SubscriptionCallback func(context.Context, string)

type Subscription interface {
	// Close stops the subscription which means the callback will no longer be called.
	// Optional; the topic will be closed when the pubsub connection is closed.
	Close()
}
