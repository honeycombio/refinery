// Package pubsubtest has PubSub stand-ins for tests.
package pubsubtest

import (
	"context"
	"errors"
	"sync"

	"github.com/honeycombio/refinery/pubsub"
)

// ErrOutageEnded is returned to a Publish that was still waiting when the
// outage ended.
var ErrOutageEnded = errors.New("pubsubtest: the outage ended before the publish completed")

// OutagePubSub is a LocalPubSub whose Publish can hang, as a publish does
// when Redis stops answering. It works like a LocalPubSub until Down is
// called. Publish then blocks until Up is called, and returns ErrOutageEnded.
type OutagePubSub struct {
	pubsub.LocalPubSub

	// HangsPastDeadline makes a blocked Publish wait for Up even after its context
	// ends, as a go-redis read on a connection that does not answer can.
	// Otherwise a blocked Publish returns the error of its context.
	HangsPastDeadline bool

	// Blocked, if set, receives a value when a Publish starts to block.
	// The send does not wait for a receiver.
	Blocked chan struct{}

	mut    sync.Mutex
	outage chan struct{} // nil when there is no outage; closed by Up
}

// Down starts an outage: later publishes block.
func (o *OutagePubSub) Down() {
	o.mut.Lock()
	defer o.mut.Unlock()
	if o.outage == nil {
		o.outage = make(chan struct{})
	}
}

// Up ends an outage. Publishes that are blocked return ErrOutageEnded, and
// later publishes work again.
func (o *OutagePubSub) Up() {
	o.mut.Lock()
	defer o.mut.Unlock()
	if o.outage != nil {
		close(o.outage)
		o.outage = nil
	}
}

func (o *OutagePubSub) Publish(ctx context.Context, topic, message string) error {
	o.mut.Lock()
	outage := o.outage
	o.mut.Unlock()
	if outage == nil {
		return o.LocalPubSub.Publish(ctx, topic, message)
	}

	select {
	case o.Blocked <- struct{}{}:
	default:
	}
	if o.HangsPastDeadline {
		<-outage
		return ErrOutageEnded
	}
	select {
	case <-outage:
		return ErrOutageEnded
	case <-ctx.Done():
		return ctx.Err()
	}
}
