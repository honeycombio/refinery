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
// pubsub.Publish(ctx, "topic", "message")
// sub := pubsub.Subscribe(ctx, "topic")
// for msg := range sub.Channel() {
// 	fmt.Println(msg)
// }
// sub.Close()  // optional
// pubsub.Close()

type PubSub interface {
	// Publish sends a message to all subscribers of the specified topic.
	Publish(ctx context.Context, topic, message string) error
	// Subscribe returns a Subscription that will receive all messages published to the specified topic.
	// There is no unsubscribe method; close the subscription to stop receiving messages.
	Subscribe(ctx context.Context, topic string) Subscription
	// Close shuts down all topics and the pubsub connection.
	Close()

	// we want to embed startstop.Starter and startstop.Stopper so that we
	// can participate in injection
	startstop.Starter
	startstop.Stopper
}

type Subscription interface {
	// Channel returns the channel that will receive all messages published to the topic.
	Channel() <-chan string
	// Close stops the subscription and closes the channel. Calling this is optional;
	// the topic will be closed when the pubsub connection is closed.
	Close()
}
