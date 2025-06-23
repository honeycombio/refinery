package types

import (
	"errors"
	"fmt"
	"io"

	collectorLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectorTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

var (
	logRequest   = &collectorLogs.ExportLogsServiceRequest{}
	traceRequest = &collectorTrace.ExportTraceServiceRequest{}
)

type WireType int

const (
	WireVarint      WireType = 0
	WireFixed64     WireType = 1
	WireLengthDelim WireType = 2
	WireFixed32     WireType = 5
)

type ProtoPayloadMap struct {
	rawData []byte
}

func NewProtoPayloadMap(raw []byte) ProtoPayloadMap {
	return ProtoPayloadMap{rawData: raw}
}

func (p *ProtoPayloadMap) Size() int {
	return len(p.rawData)
}

func (p *ProtoPayloadMap) Iterate() (protoPayloadMapIter, error) {
	return protoPayloadMapIter{remaining: p.rawData}, nil
}

type protoPayloadMapIter struct {
	remaining    []byte
	pendingValue bool
	currentWire  WireType
	fieldNum     int
	valueRaw     []byte
	currentIdx   int
}

func (m *protoPayloadMapIter) NextField() (key int, typ FieldType, err error) {
	found := false

	for !found {
		if m.pendingValue {
			m.pendingValue = false
			m.remaining = m.remaining[m.currentIdx:]
		}

		if len(m.remaining) == 0 {
			return -1, FieldTypeUnknown, io.EOF
		}

		keyInfo, n := decodeVarint(m.remaining)
		if n == 0 {
			return -1, FieldTypeUnknown, errors.New("unable to decode field key")
		}
		m.remaining = m.remaining[n:]
		m.fieldNum = int(keyInfo >> 3)
		m.currentWire = WireType(keyInfo & 0x7)

		switch m.currentWire {
		case WireVarint:
			_, n := decodeVarint(m.remaining)
			if n == 0 || n > len(m.remaining) {
				return -1, FieldTypeUnknown, errors.New("failed to decode varint")
			}
			m.valueRaw = m.remaining[:n]
			m.currentIdx = n
			typ = FieldTypeInt64
			found = true

		case WireFixed64:
			if len(m.remaining) < 8 {
				return -1, FieldTypeUnknown, io.ErrUnexpectedEOF
			}
			m.valueRaw = m.remaining[:8]
			m.currentIdx = 8
			typ = FieldTypeFloat64
			found = true

		case WireLengthDelim:
			length, n := decodeVarint(m.remaining)
			if n == 0 || int(length) > len(m.remaining[n:]) {
				return -1, FieldTypeUnknown, io.ErrUnexpectedEOF
			}

			// if it's a sub message, we need to continue iterating the fields inside it
			if isNestedField(m.remaining[n : n+int(length)]) {
				m.remaining = m.remaining[n:]
				m.currentIdx = 0
				continue
			}

			m.valueRaw = m.remaining[n : n+int(length)]
			m.currentIdx = n + int(length)
			typ = FieldTypeString
			found = true

		case WireFixed32:
			if len(m.remaining) < 4 {
				return -1, FieldTypeUnknown, io.ErrUnexpectedEOF
			}
			m.valueRaw = m.remaining[:4]
			m.currentIdx = 4
			typ = FieldTypeFloat64
			found = true

		default:
			return -1, FieldTypeUnknown, fmt.Errorf("unsupported wire type: %d", m.currentWire)
		}
	}

	m.pendingValue = true
	return m.fieldNum, typ, nil
}

func (m *protoPayloadMapIter) ValueBytes() ([]byte, error) {
	if !m.pendingValue {
		return nil, errors.New("no pending value")
	}
	return m.valueRaw, nil
}

func (m *protoPayloadMapIter) ValueInt64() (int64, error) {
	if !m.pendingValue || m.currentWire != WireVarint {
		return 0, errors.New("invalid access: expected varint value")
	}
	val, _ := decodeVarint(m.valueRaw)
	m.pendingValue = false
	return int64(val), nil
}

func (m *protoPayloadMapIter) ValueString() (string, error) {
	if !m.pendingValue || m.currentWire != WireLengthDelim {
		return "", errors.New("invalid access: expected length-delimited string")
	}
	m.pendingValue = false
	return string(m.valueRaw), nil
}

func decodeVarint(buf []byte) (uint64, int) {
	var result uint64
	var s uint
	for i, b := range buf {
		// check if the byte is a continuation byte
		if b < 0x80 {
			// this is the last byte of the varint
			if i > 9 || i == 9 && b > 1 {
				return 0, 0 // overflow
			}
			return result | uint64(b)<<s, i + 1
		}
		// Extract the 7 bits and shift them into the result
		result |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

func isNestedField(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	// TODO: how to determine if the data is a nested field and not just a []byte or string?
	return false
}
