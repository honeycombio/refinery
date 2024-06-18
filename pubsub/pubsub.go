package pubsub

import (
	"context"

	"github.com/facebookgo/startstop"
)

// general usage:
// pubsub := pubsub.NewXXXPubSub()
// topic := pubsub.NewTopic(ctx, "name")
// topic.Publish(ctx, "message")
// ch := topic.Subscribe(ctx)
// for msg := range ch {
// 	fmt.Println(msg)
// }
// topic.Close() // optional if you want to close the topic independently
// pubsub.Close()

type PubSub interface {
	// NewTopic creates a new topic with the given name.
	// When a topic is created, it is stored in the topics map and a goroutine is
	// started to listen for messages on the topic; each message is sent to all
	// subscribers to the topic. Close the topic to stop the goroutine and all subscriber
	// channels.
	NewTopic(ctx context.Context, topic string) Topic
	// Close shuts down all topics and the pubsub connection.
	Close()

	// we want to embed startstop.Starter and startstop.Stopper so that we
	// can participate in injection
	startstop.Starter
	startstop.Stopper
}

type Topic interface {
	// Publish sends a message to all subscribers of the topic.
	Publish(ctx context.Context, message string) error
	// Subscribe returns a channel that will receive all messages published to the topic.
	// There is no unsubscribe method; close the topic to stop receiving messages.
	Subscribe(ctx context.Context) <-chan string
	// Close shuts down the topic and all subscriber channels. Calling this is optional;
	// the topic will be closed when the pubsub connection is closed.
	Close()
}
