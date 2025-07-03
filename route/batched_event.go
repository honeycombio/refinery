package route

import (
	"bytes"
	"time"

	"github.com/honeycombio/refinery/types"
	"github.com/tinylib/msgp/msgp"
)

type batchedEvent struct {
	Timestamp        string        `json:"time"`
	MsgPackTimestamp *time.Time    `msgpack:"time,omitempty"`
	SampleRate       int64         `json:"samplerate" msgpack:"samplerate"`
	Data             types.Payload `json:"data" msgpack:"data"`
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
				if err != nil {
					return
				}
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
type batchedEvents []batchedEvent

// UnmarshalMsg implements msgp.Unmarshaler
func (b *batchedEvents) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var totalValues uint32
	totalValues, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap(*b) >= int(totalValues) {
		*b = (*b)[:totalValues]
	} else {
		*b = make(batchedEvents, totalValues)
	}
	for i := range *b {
		bts, err = (*b)[i].UnmarshalMsg(bts)
		if err != nil {
			err = msgp.WrapError(err, i)
			return
		}
	}
	o = bts
	return
}

// UnmarshalMsgWithMetadata is an optimized version that extracts metadata during unmarshaling
func (b *batchedEvents) UnmarshalMsgWithMetadata(bts []byte, traceIdFieldNames, parentIdFieldNames []string) (o []byte, err error) {
	var totalValues uint32
	totalValues, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	if cap(*b) >= int(totalValues) {
		*b = (*b)[:totalValues]
	} else {
		*b = make(batchedEvents, totalValues)
	}
	for i := range *b {
		bts, err = (*b)[i].UnmarshalMsgWithMetadata(bts, traceIdFieldNames, parentIdFieldNames)
		if err != nil {
			err = msgp.WrapError(err, i)
			return
		}
	}
	o = bts
	return
}

// UnmarshalMsgWithMetadata is an optimized version that unmarshals and extracts metadata in one pass
func (b *batchedEvent) UnmarshalMsgWithMetadata(bts []byte, traceIdFieldNames, parentIdFieldNames []string) (o []byte, err error) {
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
				if err != nil {
					return
				}
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
			// Use the optimized UnmarshalMsgWithMetadata for the payload
			bts, err = b.Data.UnmarshalMsgWithMetadata(bts, traceIdFieldNames, parentIdFieldNames)
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
