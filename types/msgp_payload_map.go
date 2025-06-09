package types

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

type FieldType int

const (
	FieldTypeUnknown = iota
	FieldTypeInt64
	FieldTypeFloat64
	FieldTypeString
	FieldTypeBool

	// Arrays, maps, other stuff supported by the wire protocols but not
	// expected to be very common.
	FieldTypeOther
)

// A wrapper for a serialized messagepack map. Reading its values can be far
// cheaper than deserializing into a real map, but the gains disappear quickly
// if you do too many iterations. It is recommended to retrieve values in batches
// and memoize anything that will be used more than once.
type MsgpPayloadMap struct {
	rawData msgpack.RawMessage
}

func NewMessagePackPayloadMap(raw msgpack.RawMessage) MsgpPayloadMap {
	return MsgpPayloadMap{
		rawData: raw,
	}
}

func (m *MsgpPayloadMap) Iterate() (msgpPayloadMapIter, error) {
	decoder := msgpack.GetDecoder()
	decoder.Reset(bytes.NewReader(m.rawData))

	n, err := decoder.DecodeMapLen()
	if err != nil {
		msgpack.PutDecoder(decoder)
		return msgpPayloadMapIter{}, err
	}
	if n < 1 {
		return msgpPayloadMapIter{}, nil
	}

	return msgpPayloadMapIter{
		decoder:   decoder,
		remaining: n,
	}, nil
}

type msgpPayloadMapIter struct {
	decoder      *msgpack.Decoder
	remaining    int
	pendingValue bool
}

// Call this when you're done with the iterator to return the decoder to the pool.
func (m *msgpPayloadMapIter) Done() {
	m.remaining = 0
	if m.decoder != nil {
		msgpack.PutDecoder(m.decoder)
		m.decoder = nil
	}
}

// Returns the key string as []byte and type of the next map value.
// Note that the key slice will be invalidated by the next call to NextKey()
// or completing iteration. It must not be stored.
// Returns EOF when done.
// After any error, the iterator is invalid and will always return EOF.
// TODO make this a zero-copy operation, why may involve using a different
// library, since vmihailenco doesn't seem to offer a way to do that.
func (m *msgpPayloadMapIter) NextKey() (key []byte, typ FieldType, err error) {
	if m.remaining <= 0 {
		return nil, FieldTypeUnknown, io.EOF
	}

	// The last value was never consumed, that's common. Skip it.
	if m.pendingValue {
		err = m.decoder.Skip()
		if err != nil {
			m.Done()
			return nil, FieldTypeUnknown, err
		}
	}

	key, err = m.decoder.DecodeBytes()
	if err != nil {
		m.Done()
		return nil, FieldTypeUnknown, err
	}

	code, err := m.decoder.PeekCode()
	if err != nil {
		m.Done()
		return nil, FieldTypeUnknown, err
	}

	typ, err = msgPackCodeToFieldType(code)
	if err != nil {
		m.Done()
		return nil, FieldTypeUnknown, err
	}

	m.pendingValue = true
	return key, typ, nil
}

// The Value functions return the value corresponding to the previous call to
// NextKey(), if any. If the value is not of the expected type, return an error.
func (m *msgpPayloadMapIter) ValueInt64() (int64, error) {
	if !m.pendingValue {
		return 0, errors.New("no pending value")
	}
	m.pendingValue = false
	return m.decoder.DecodeInt64()
}

func (m *msgpPayloadMapIter) ValueFloat64() (float64, error) {
	if !m.pendingValue {
		return 0, errors.New("no pending value")
	}
	m.pendingValue = false
	return m.decoder.DecodeFloat64()
}

func (m *msgpPayloadMapIter) ValueBool() (bool, error) {
	if !m.pendingValue {
		return false, errors.New("no pending value")
	}
	m.pendingValue = false
	return m.decoder.DecodeBool()
}

func (m *msgpPayloadMapIter) ValueString() (string, error) {
	if !m.pendingValue {
		return "", errors.New("no pending value")
	}
	m.pendingValue = false
	return m.decoder.DecodeString()
}

func (m *msgpPayloadMapIter) ValueOther() (any, error) {
	if !m.pendingValue {
		return nil, errors.New("no pending value")
	}
	m.pendingValue = false
	return m.decoder.DecodeInterfaceLoose()
}

func msgPackCodeToFieldType(c byte) (FieldType, error) {
	if msgpcode.IsFixedNum(c) {
		return FieldTypeInt64, nil
	}
	if msgpcode.IsFixedMap(c) {
		return FieldTypeOther, nil
	}
	if msgpcode.IsFixedArray(c) {
		return FieldTypeOther, nil
	}
	if msgpcode.IsFixedString(c) {
		return FieldTypeString, nil
	}

	switch c {
	case msgpcode.Nil:
		return FieldTypeOther, nil
	case msgpcode.False, msgpcode.True:
		return FieldTypeBool, nil
	case msgpcode.Float, msgpcode.Double:
		return FieldTypeFloat64, nil
	case msgpcode.Uint8, msgpcode.Uint16, msgpcode.Uint32, msgpcode.Uint64,
		msgpcode.Int8, msgpcode.Int16, msgpcode.Int32, msgpcode.Int64:
		return FieldTypeInt64, nil
	case msgpcode.Str8, msgpcode.Str16, msgpcode.Str32,
		msgpcode.Bin8, msgpcode.Bin16, msgpcode.Bin32:
		return FieldTypeString, nil
	case msgpcode.Array16, msgpcode.Array32,
		msgpcode.Map16, msgpcode.Map32,
		msgpcode.FixExt1, msgpcode.FixExt2, msgpcode.FixExt4, msgpcode.FixExt8, msgpcode.FixExt16,
		msgpcode.Ext8, msgpcode.Ext16, msgpcode.Ext32:
		return FieldTypeOther, nil
	default:
		return FieldTypeUnknown, fmt.Errorf("msgpack: unknown code %x decoding interface{}", c)
	}
}
