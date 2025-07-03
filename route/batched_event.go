package route

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
	"github.com/tinylib/msgp/msgp"
)

type batchedEvent struct {
	Timestamp        string        `json:"time"`
	MsgPackTimestamp *time.Time    `msgpack:"time,omitempty"`
	SampleRate       int64         `json:"samplerate" msgpack:"samplerate"`
	Data             types.Payload `json:"data" msgpack:"data"`
	cfg              config.Config `json:"-" msgpack:"-"`
}

func (b *batchedEvent) getEventTime() time.Time {
	if b.MsgPackTimestamp != nil {
		return b.MsgPackTimestamp.UTC()
	}

	return getEventTime(b.Timestamp)
}

func (b *batchedEvent) getSampleRate() uint {
	if b.SampleRate == 0 {
		return defaultSampleRate
	}
	return uint(b.SampleRate)
}

func (b *batchedEvent) UnmarshalJSON(data []byte) error {
	type tempEvent struct {
		Timestamp  string          `json:"time"`
		SampleRate int64           `json:"samplerate"`
		Data       json.RawMessage `json:"data"`
	}

	var temp tempEvent
	err := json.Unmarshal(data, &temp)
	if err != nil {
		return err
	}

	// Copy the simple fields
	b.Timestamp = temp.Timestamp
	b.SampleRate = temp.SampleRate

	// Initialize Data with config and then unmarshal the raw JSON into it
	b.Data = types.NewPayload(b.cfg, nil)
	err = json.Unmarshal(temp.Data, &b.Data)
	if err != nil {
		return err
	}

	return nil
}

// UnmarshalMsg implements msgp.Unmarshaler
// Based on generated code from the msgp tool, but modified for clarity and to
// avoid the unnecessary unsafe string conversion.
// For instructions on how to generate this sort of code, see
// https://pkg.go.dev/github.com/tinylib/msgp#section-readme
func (b *batchedEvent) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	var fieldsRemaining uint32
	fieldsRemaining, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for fieldsRemaining > 0 {
		fieldsRemaining--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch {
		case bytes.Equal(field, []byte("time")):
			if msgp.IsNil(bts) {
				bts, err = msgp.ReadNilBytes(bts)
				b.MsgPackTimestamp = nil
			} else {
				if b.MsgPackTimestamp == nil {
					b.MsgPackTimestamp = new(time.Time)
				}
				*b.MsgPackTimestamp, bts, err = msgp.ReadTimeBytes(bts)
			}
		case bytes.Equal(field, []byte("samplerate")):
			b.SampleRate, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SampleRate")
				return
			}
		case bytes.Equal(field, []byte("data")):
			b.Data = types.NewPayload(b.cfg, nil)
			bts, err = b.Data.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "Data")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Create a type for []batchedEvent so we can give it an unmarshaler.
type batchedEvents struct {
	events []batchedEvent
	cfg    config.Config
}

func newBatchedEvents(cfg config.Config) *batchedEvents {
	return &batchedEvents{
		events: make([]batchedEvent, 0),
		cfg:    cfg,
	}
}

// UnmarshalMsg implements msgp.Unmarshaler
func (b *batchedEvents) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var totalValues uint32
	totalValues, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	b.events = make([]batchedEvent, totalValues)
	for i := range b.events {
		b.events[i].cfg = b.cfg
		bts, err = b.events[i].UnmarshalMsg(bts)
		if err != nil {
			err = msgp.WrapError(err, i)
			return
		}
	}
	o = bts
	return
}

func (b *batchedEvents) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.events)
}

func (b *batchedEvents) UnmarshalJSON(data []byte) error {
	var rawEvents []json.RawMessage
	err := json.Unmarshal(data, &rawEvents)
	if err != nil {
		return err
	}

	b.events = make([]batchedEvent, len(rawEvents))
	for i := range b.events {
		b.events[i].cfg = b.cfg

		err = json.Unmarshal(rawEvents[i], &b.events[i])
		if err != nil {
			return err
		}
	}

	return nil
}
