package route

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
	"github.com/tinylib/msgp/msgp"
	"github.com/valyala/fastjson"
)

var fastJsonParserPool fastjson.ParserPool

type batchedEvent struct {
	Timestamp           string        `json:"time"`
	MsgPackTimestamp    *time.Time    `msgpack:"time,omitempty"`
	SampleRate          int64         `json:"samplerate" msgpack:"samplerate"`
	Data                types.Payload `json:"data" msgpack:"data"`
	cfg                 config.Config `json:"-" msgpack:"-"`
	coreFieldsExtractor types.CoreFieldsUnmarshaler
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
	// This method is now primarily used as a fallback for individual event unmarshaling
	// Most of the time, events are unmarshaled via batchedEvents.UnmarshalJSON
	type tempEvent struct {
		Timestamp  string          `json:"time"`
		SampleRate int64           `json:"samplerate"`
		Data       json.RawMessage `json:"data"`
	}

	var temp tempEvent
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	b.Timestamp = temp.Timestamp
	b.SampleRate = temp.SampleRate
	b.Data = types.NewPayload(b.cfg, nil)

	// Convert data field to MessagePack and use optimized unmarshaling
	buf := httpBodyBufferPool.Get().(*bytes.Buffer)
	defer recycleHTTPBodyBuffer(buf)

	msgpackData, err := types.JSONToMessagePack(buf.Bytes()[:0], temp.Data)
	if err != nil {
		return err
	}

	_, err = b.coreFieldsExtractor.UnmarshalPayload(msgpackData, &b.Data)
	return err
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
				if err != nil {
					err = msgp.WrapError(err, "MsgPackTimestamp")
					return
				}
			}
		case bytes.Equal(field, []byte("samplerate")):
			b.SampleRate, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SampleRate")
				return
			}
		case bytes.Equal(field, []byte("data")):
			b.Data = types.NewPayload(b.cfg, nil) // Initialize with config
			bts, err = b.coreFieldsExtractor.UnmarshalPayload(bts, &b.Data)
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
	events              []batchedEvent
	cfg                 config.Config
	coreFieldsExtractor types.CoreFieldsUnmarshaler
}

func newBatchedEvents(cfg config.Config, apiKey, env, dataset string) *batchedEvents {
	return &batchedEvents{
		events:              make([]batchedEvent, 0),
		cfg:                 cfg,
		coreFieldsExtractor: types.NewCoreFieldsUnmarshaler(cfg, apiKey, env, dataset),
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
		b.events[i].coreFieldsExtractor = b.coreFieldsExtractor
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
	parser := fastJsonParserPool.Get()
	defer fastJsonParserPool.Put(parser)

	v, err := parser.ParseBytes(data)
	if err != nil {
		return err
	}

	if v.Type() != fastjson.TypeArray {
		return fmt.Errorf("expected JSON array")
	}

	arr, err := v.Array()
	if err != nil {
		return err
	}

	b.events = make([]batchedEvent, len(arr))
	for i, eventValue := range arr {
		b.events[i].cfg = b.cfg
		b.events[i].coreFieldsExtractor = b.coreFieldsExtractor

		// Parse each event directly using fastjson
		err = b.unmarshalBatchedEventFromFastJSON(&b.events[i], eventValue)
		if err != nil {
			return err
		}
	}

	return nil
}

// unmarshalBatchedEventFromFastJSON populates a batchedEvent from a fastjson.Value
func (b *batchedEvents) unmarshalBatchedEventFromFastJSON(event *batchedEvent, v *fastjson.Value) error {
	if v.Type() != fastjson.TypeObject {
		return fmt.Errorf("expected JSON object for event")
	}

	obj, err := v.Object()
	if err != nil {
		return err
	}

	// Initialize Data with config
	event.Data = types.NewPayload(event.cfg, nil)

	var dataValue *fastjson.Value

	// Visit each field in the event object
	obj.Visit(func(key []byte, v *fastjson.Value) {
		if err != nil {
			return
		}

		switch string(key) {
		case "time":
			if v.Type() == fastjson.TypeString {
				s, _ := v.StringBytes()
				event.Timestamp = string(s)
			}
		case "samplerate":
			if v.Type() == fastjson.TypeNumber {
				event.SampleRate = v.GetInt64()
			}
		case "data":
			if v.Type() == fastjson.TypeObject {
				dataValue = v
			}
		}
	})

	if err != nil {
		return err
	}

	// Convert data field to MessagePack and use optimized unmarshaling
	if dataValue != nil {
		buf := httpBodyBufferPool.Get().(*bytes.Buffer)
		defer recycleHTTPBodyBuffer(buf)

		// Use AppendJSONValue directly to avoid expensive MarshalTo() call
		msgpackData, err := types.AppendJSONValue(buf.Bytes()[:0], dataValue)
		if err != nil {
			return err
		}

		// Use the same optimized unmarshaling logic as UnmarshalMsg
		_, err = event.coreFieldsExtractor.UnmarshalPayload(msgpackData, &event.Data)
		return err
	}

	return nil
}

