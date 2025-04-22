package transmit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/honeycombio/libhoney-go/transmission"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/pubsub"
)

type GooglePeerTransmission struct {
	Config config.Config `inject:""`
	PubSub pubsub.PubSub `inject:""`
	Logger logger.Logger `inject:""`

	EventHandler func(context.Context, transmission.Event, any) error

	responses        chan transmission.Response
	blockOnResponses bool
}

var _ transmission.Sender = (*GooglePeerTransmission)(nil)

func (t *GooglePeerTransmission) Start() error {
	t.PubSub.Subscribe(context.Background(), fmt.Sprintf("peer-%s", t.Config.GetRedisIdentifier()), func(ctx context.Context, payload string) {
		var ev transmission.Event
		err := json.Unmarshal([]byte(payload), &ev)
		if err != nil {
			return
		}
		t.EventHandler(ctx, ev, nil)
	})
	return nil
}

func (t *GooglePeerTransmission) Stop() error {
	return nil
}

func (t *GooglePeerTransmission) Add(ev *transmission.Event) {
	serialized, err := ev.MarshalJSON()
	if err != nil {
		return
	}
	t.PubSub.Publish(context.Background(), fmt.Sprintf("peer-%s", ev.APIHost), string(serialized))
}

func (t *GooglePeerTransmission) Flush() error {
	return nil
}

func (t *GooglePeerTransmission) TxResponses() chan transmission.Response {
	return t.responses
}

func (t *GooglePeerTransmission) SendResponse(r transmission.Response) bool {
	if t.blockOnResponses {
		t.responses <- r
	} else {
		select {
		case t.responses <- r:
		default:
			return true
		}
	}
	return false
}
