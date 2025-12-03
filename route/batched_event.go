package route

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/tinylib/msgp/msgp"
	"github.com/valyala/fastjson"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
)

var fastJsonParserPool fastjson.ParserPool

type batchedEvent struct {
	Timestamp           string        `json:"time"`
	MsgPackTimestamp    *time.Time    `msgpack:"time,omitempty"`
	SampleRate          int64         `json:"samplerate" msgpack:"samplerate"`
	Data                types.Payload `json:"data" msgpack:"data"`
	Dataset             string        `json:"dataset,omitempty" msgpack:"dataset,omitempty"`
	cfg                 config.Config `json:"-" msgpack:"-"`
	coreFieldsExtractor types.CoreFieldsUnmarshaler
	// rawDataBytes stores the raw msgpack bytes for the data field, allowing
	// deferred payload creation with the correct per-dataset unmarshaler
	rawDataBytes []byte `json:"-" msgpack:"-"`
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
			// Store the starting position to capture raw bytes
			dataStart := bts

			// Skip over the data field to find its end
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err, "Data")
				return
			}

			// Save raw msgpack bytes for later processing with correct unmarshaler
			dataLen := len(dataStart) - len(bts)
			b.rawDataBytes = make([]byte, dataLen)
			copy(b.rawDataBytes, dataStart[:dataLen])
		case bytes.Equal(field, []byte("dataset")):
			b.Dataset, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Dataset")
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

func newBatchedEvents(opts types.CoreFieldsUnmarshalerOptions) *batchedEvents {
	return &batchedEvents{
		events:              make([]batchedEvent, 0),
		cfg:                 opts.Config,
		coreFieldsExtractor: types.NewCoreFieldsUnmarshaler(opts),
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

var bytesPool = sync.Pool{
	New: func() any {
		slice := make([]byte, 0, 128)
		return &slice
	},
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

	// Visit each field in the event object
	var dataValue *fastjson.Value
	obj.Visit(func(key []byte, v *fastjson.Value) {
		switch string(key) {
		case "time":
			if v.Type() == fastjson.TypeString {
				s := v.GetStringBytes()
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
		case "dataset":
			if v.Type() == fastjson.TypeString {
				s := v.GetStringBytes()
				event.Dataset = string(s)
			}
		}
	})

	// Convert data field to MessagePack bytes for later processing
	if dataValue != nil {
		buf := bytesPool.Get().(*[]byte)
		defer func() {
			*buf = (*buf)[:0]
			bytesPool.Put(buf)
		}()

		*buf, err = types.AppendJSONValue(*buf, dataValue)
		if err != nil {
			return err
		}

		// Save raw msgpack bytes for later processing with correct unmarshaler
		event.rawDataBytes = make([]byte, len(*buf))
		copy(event.rawDataBytes, *buf)
	}

	return nil
}
