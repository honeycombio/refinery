package types

import (
	"errors"
	"fmt"
	"io"

	"github.com/tinylib/msgp/msgp"
)

// newMsgpPayloadMapIter creates an iterator for a serialized messagepack map.
// Reading its values can be far cheaper than deserializing into a real map, but the gains
// disappear quickly if you do too many iterations. It is recommended to retrieve values
// in batches and memoize anything that will be used more than once.
func newMsgpPayloadMapIter(rawData []byte) (msgpPayloadMapIter, error) {
	if len(rawData) == 0 {
		return msgpPayloadMapIter{}, nil
	}
	n, remaining, err := msgp.ReadMapHeaderBytes(rawData)
	if err != nil {
		return msgpPayloadMapIter{}, err
	}
	if n < 1 {
		return msgpPayloadMapIter{}, nil
	}

	return msgpPayloadMapIter{
		remaining: remaining,
	}, nil
}

type msgpPayloadMapIter struct {
	remaining    []byte
	pendingValue bool
}

// Returns the key string as []byte and type of the next map value.
// Note that the key slice may be invalidated by the next call to nextKey()
// or completing iteration. It must not be stored.
// Returns EOF when finished.
func (m *msgpPayloadMapIter) nextKey() (key []byte, typ FieldType, err error) {
	// The last value was never consumed, that's common. Skip it.
	if m.pendingValue {
		m.remaining, err = msgp.Skip(m.remaining)
		if err != nil {
			return nil, FieldTypeUnknown, err
		}
	}

	if len(m.remaining) == 0 {
		return nil, FieldTypeUnknown, io.EOF
	}

	key, m.remaining, err = msgp.ReadMapKeyZC(m.remaining)
	if err != nil {
		return nil, FieldTypeUnknown, err
	}

	typ, err = msgpTypeToFieldType(msgp.NextType(m.remaining))
	if err != nil {
		return nil, FieldTypeUnknown, err
	}

	m.pendingValue = true
	return key, typ, nil
}

// The Value functions return the value corresponding to the previous call to
// NextKey(), if any. valueAny() returns any type as any.
func (m *msgpPayloadMapIter) valueAny() (any, error) {
	if !m.pendingValue {
		return nil, errors.New("no pending value")
	}
	m.pendingValue = false
	val, remaining, err := msgp.ReadIntfBytes(m.remaining)
	if err == nil {
		m.remaining = remaining
	} else {
		m.remaining, _ = msgp.Skip(m.remaining)
	}
	return val, err
}

// Return the raw serialized bytes for the next value, without copying.
func (m *msgpPayloadMapIter) valueSerializedBytesZC() ([]byte, error) {
	if !m.pendingValue {
		return nil, errors.New("no pending value")
	}
	m.pendingValue = false

	var raw []byte
	remainder, err := msgp.Skip(m.remaining)
	if err == nil {
		raw = m.remaining[:len(m.remaining)-len(remainder)]
	}
	m.remaining = remainder
	return raw, err
}

func msgpTypeToFieldType(t msgp.Type) (FieldType, error) {
	switch t {
	case msgp.StrType, msgp.BinType:
		return FieldTypeString, nil

	case msgp.IntType, msgp.UintType:
		return FieldTypeInt64, nil

	case msgp.Float64Type, msgp.Float32Type, msgp.NumberType:
		return FieldTypeFloat64, nil

	case msgp.BoolType:
		return FieldTypeBool, nil

	case msgp.NilType, msgp.MapType, msgp.ArrayType, msgp.DurationType,
		msgp.ExtensionType, msgp.Complex64Type, msgp.Complex128Type, msgp.TimeType:
		return FieldTypeOther, nil

	default:
		return FieldTypeUnknown, fmt.Errorf("msgpack: unknown msgp type %x", t)
	}
}
