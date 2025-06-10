package types

import (
	"errors"
	"fmt"
	"io"

	"github.com/tinylib/msgp/msgp"
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
	rawData []byte
}

func NewMessagePackPayloadMap(raw []byte) MsgpPayloadMap {
	return MsgpPayloadMap{
		rawData: raw,
	}
}

func (m *MsgpPayloadMap) Iterate() (msgpPayloadMapIter, error) {
	n, remaining, err := msgp.ReadMapHeaderBytes(m.rawData)
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
// Note that the key slice may be invalidated by the next call to NextKey()
// or completing iteration. It must not be stored.
// Returns EOF when finished.
func (m *msgpPayloadMapIter) NextKey() (key []byte, typ FieldType, err error) {
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
// NextKey(), if any. ValueAny() returns any type as any.
func (m *msgpPayloadMapIter) ValueAny() (any, error) {
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

// The typed values decode data as the requested type, if possible, but don't
// attempt to coerce non-matching types. If the value is not of the expected
// type, returns an error.
func (m *msgpPayloadMapIter) ValueInt64() (int64, error) {
	if !m.pendingValue {
		return 0, errors.New("no pending value")
	}
	m.pendingValue = false
	val, remaining, err := msgp.ReadInt64Bytes(m.remaining)
	if err == nil {
		m.remaining = remaining
	} else {
		m.remaining, _ = msgp.Skip(m.remaining)
	}
	return val, err
}

func (m *msgpPayloadMapIter) ValueFloat64() (float64, error) {
	if !m.pendingValue {
		return 0, errors.New("no pending value")
	}
	m.pendingValue = false
	val, remaining, err := msgp.ReadFloat64Bytes(m.remaining)
	if err == nil {
		m.remaining = remaining
	} else {
		m.remaining, _ = msgp.Skip(m.remaining)
	}
	return val, err
}

func (m *msgpPayloadMapIter) ValueBool() (bool, error) {
	if !m.pendingValue {
		return false, errors.New("no pending value")
	}
	m.pendingValue = false
	val, remaining, err := msgp.ReadBoolBytes(m.remaining)
	if err == nil {
		m.remaining = remaining
	} else {
		m.remaining, _ = msgp.Skip(m.remaining)
	}
	return val, err
}

func (m *msgpPayloadMapIter) ValueString() (string, error) {
	if !m.pendingValue {
		return "", errors.New("no pending value")
	}
	m.pendingValue = false
	val, remaining, err := msgp.ReadStringBytes(m.remaining)
	if err == nil {
		m.remaining = remaining
	} else {
		m.remaining, _ = msgp.Skip(m.remaining)
	}
	return val, err
}

func msgpTypeToFieldType(t msgp.Type) (FieldType, error) {
	switch t {
	case msgp.StrType, msgp.BinType:
		return FieldTypeString, nil

	case msgp.MapType, msgp.ArrayType, msgp.NilType, msgp.DurationType,
		msgp.ExtensionType, msgp.Complex64Type, msgp.Complex128Type, msgp.TimeType:
		return FieldTypeOther, nil

	case msgp.Float64Type, msgp.Float32Type, msgp.NumberType:
		return FieldTypeFloat64, nil

	case msgp.BoolType:
		return FieldTypeBool, nil

	case msgp.IntType, msgp.UintType:
		return FieldTypeInt64, nil

	default:
		return FieldTypeUnknown, fmt.Errorf("msgpack: unknown msgp type %x", t)
	}
}
