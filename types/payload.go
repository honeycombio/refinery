package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"maps"

	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

// Metadata field constants
const (
	MetaSignalType                = "meta.signal_type"
	MetaTraceID                   = "meta.trace_id"
	MetaAnnotationType            = "meta.annotation_type"
	MetaRefineryProbe             = "meta.refinery.probe"
	MetaRefineryRoot              = "meta.refinery.root"
	MetaRefineryIncomingUserAgent = "meta.refinery.incoming_user_agent"
	MetaRefinerySendBy            = "meta.refinery.send_by"
	MetaRefinerySpanDataSize      = "meta.refinery.span_data_size"
	MetaRefineryMinSpan           = "meta.refinery.min_span"
	MetaRefineryForwarded         = "meta.refinery.forwarded"
	MetaRefineryExpiredTrace      = "meta.refinery.expired_trace"
)

type FieldType int

const (
	FieldTypeUnknown FieldType = iota
	FieldTypeInt64
	FieldTypeFloat64
	FieldTypeString
	FieldTypeBool

	// Arrays, maps, other stuff supported by the wire protocols but not
	// expected to be very common.
	FieldTypeOther
)

type nullableBool struct {
	HasValue bool
	Value    bool
}

func (nb *nullableBool) Set(value bool) {
	nb.HasValue = true
	nb.Value = value
}

func (nb *nullableBool) Unset() {
	nb.HasValue = false
	nb.Value = false
}

type Payload struct {
	// A serialized messagepack map used to source fields.
	msgpMap MsgpPayloadMap

	// Deserialized fields, either from the internal msgpMap, or set externally.
	memoizedFields map[string]any
	// missingFields is a set of fields that were not found in the payload.
	// this is used to avoid repeatedly deserializing fields that are not present.
	missingFields map[string]struct{}

	// Cached metadata fields for efficient access
	MetaSignalType                string       // meta.signal_type
	MetaTraceID                   string       // meta.trace_id
	MetaAnnotationType            string       // meta.annotation_type
	MetaRefineryProbe             nullableBool // meta.refinery.probe
	MetaRefineryRoot              nullableBool // meta.refinery.root
	MetaRefineryIncomingUserAgent string       // meta.refinery.incoming_user_agent
	MetaRefinerySendBy            int64        // meta.refinery.send_by (Unix timestamp)
	MetaRefinerySpanDataSize      int64        // meta.refinery.span_data_size
	MetaRefineryMinSpan           nullableBool // meta.refinery.min_span
	MetaRefineryForwarded         string       // meta.refinery.forwarded
	MetaRefineryExpiredTrace      nullableBool // meta.refinery.expired_trace
}

// ExtractMetadata populates the cached metadata fields from the payload data.
// This MUST be called manually after creating or unmarshaling a non-empty Payload
// to populate the metadata fields. The traceIdFieldNames and parentIdFieldNames parameters
// are optional and used to extract trace ID and determine if the span is a root span.
func (p *Payload) ExtractMetadata(traceIdFieldNames, parentIdFieldNames []string) {
	// Track if we found parent ID for root span determination
	var parentIdFound bool
	var parentIdValue string

	// For memoized fields, directly access the map
	if p.memoizedFields != nil {
		for key, value := range p.memoizedFields {
			switch key {
			case MetaSignalType:
				if v, ok := value.(string); ok {
					p.MetaSignalType = v
				}
			case MetaTraceID:
				if v, ok := value.(string); ok {
					p.MetaTraceID = v
				}
			case MetaAnnotationType:
				if v, ok := value.(string); ok {
					p.MetaAnnotationType = v
				} else if v, ok := value.(int); ok {
					// Handle int values that come from ExtractDecisionContext
					switch v {
					case int(SpanAnnotationTypeUnknown):
						p.MetaAnnotationType = ""
					case int(SpanAnnotationTypeSpanEvent):
						p.MetaAnnotationType = "span_event"
					case int(SpanAnnotationTypeLink):
						p.MetaAnnotationType = "link"
					default:
						p.MetaAnnotationType = ""
					}
				}
			case MetaRefineryProbe:
				if v, ok := value.(bool); ok {
					p.MetaRefineryProbe.Set(v)
				}
			case MetaRefineryRoot:
				if v, ok := value.(bool); ok {
					p.MetaRefineryRoot.Set(v)
				}
			case MetaRefineryIncomingUserAgent:
				if v, ok := value.(string); ok {
					p.MetaRefineryIncomingUserAgent = v
				}
			case MetaRefinerySendBy:
				if v, ok := value.(int64); ok {
					p.MetaRefinerySendBy = v
				}
			case MetaRefinerySpanDataSize:
				if v, ok := value.(int64); ok {
					p.MetaRefinerySpanDataSize = v
				}
			case MetaRefineryMinSpan:
				if v, ok := value.(bool); ok {
					p.MetaRefineryMinSpan.Set(v)
				}
			case MetaRefineryForwarded:
				if v, ok := value.(string); ok {
					p.MetaRefineryForwarded = v
				}
			case MetaRefineryExpiredTrace:
				if v, ok := value.(bool); ok {
					p.MetaRefineryExpiredTrace.Set(v)
				}
			default:
				// Check if this is a trace ID field
				if p.MetaTraceID == "" {
					for _, field := range traceIdFieldNames {
						if key == field {
							if v, ok := value.(string); ok && v != "" {
								p.MetaTraceID = v
							}
							break
						}
					}
				}

				// Check if this is a parent ID field
				if !parentIdFound {
					for _, field := range parentIdFieldNames {
						if key == field {
							if v, ok := value.(string); ok && v != "" {
								parentIdFound = true
								parentIdValue = v
							}
							break
						}
					}
				}
			}
		}
	}

	// For msgpMap fields, use the iterator
	if p.msgpMap.Size() > 0 {
		iter, err := p.msgpMap.Iterate()
		if err != nil {
			return
		}

		// Iterate through all fields looking for metadata
		for {
			key, fieldType, err := iter.NextKey()
			if err != nil {
				break
			}

			// Use bytes comparison for known metadata fields to avoid string allocation
			switch fieldType {
			case FieldTypeString:
				if bytes.HasPrefix(key, []byte("meta.")) {
					switch {
					case bytes.Equal(key, []byte(MetaSignalType)):
						if value, err := iter.ValueString(); err == nil {
							p.MetaSignalType = value
						}
					case bytes.Equal(key, []byte(MetaTraceID)):
						if value, err := iter.ValueString(); err == nil {
							p.MetaTraceID = value
						}
					case bytes.Equal(key, []byte(MetaAnnotationType)):
						if value, err := iter.ValueString(); err == nil {
							p.MetaAnnotationType = value
						}
					case bytes.Equal(key, []byte(MetaRefineryIncomingUserAgent)):
						if value, err := iter.ValueString(); err == nil {
							p.MetaRefineryIncomingUserAgent = value
						}
					case bytes.Equal(key, []byte(MetaRefineryForwarded)):
						if value, err := iter.ValueString(); err == nil {
							p.MetaRefineryForwarded = value
						}
					}
				}
				// Check if this is a trace ID field
				if p.MetaTraceID == "" && sliceContains(traceIdFieldNames, key) {
					if value, err := iter.ValueString(); err == nil {
						p.MetaTraceID = value
					}
					break
				}

				// Check if this is a parent ID field
				if !parentIdFound && sliceContains(parentIdFieldNames, key) {
					if value, err := iter.ValueString(); err == nil && value != "" {
						parentIdFound = true
						parentIdValue = value
					}
					break
				}
			case FieldTypeBool:
				switch {
				case bytes.Equal(key, []byte(MetaRefineryMinSpan)):
					if value, err := iter.ValueBool(); err == nil {
						p.MetaRefineryMinSpan.Set(value)
					}
				case bytes.Equal(key, []byte(MetaRefineryExpiredTrace)):
					if value, err := iter.ValueBool(); err == nil {
						p.MetaRefineryExpiredTrace.Set(value)
					}
				case bytes.Equal(key, []byte(MetaRefineryProbe)):
					if value, err := iter.ValueBool(); err == nil {
						p.MetaRefineryProbe.Set(value)
					}
				case bytes.Equal(key, []byte(MetaRefineryRoot)):
					if value, err := iter.ValueBool(); err == nil {
						p.MetaRefineryRoot.Set(value)
					}
				}
			case FieldTypeInt64:
				switch {
				case bytes.Equal(key, []byte(MetaRefinerySendBy)):
					if value, err := iter.ValueInt64(); err == nil {
						p.MetaRefinerySendBy = value
					}
				case bytes.Equal(key, []byte(MetaRefinerySpanDataSize)):
					if value, err := iter.ValueInt64(); err == nil {
						p.MetaRefinerySpanDataSize = value
					}
				}
			}
		}
	}

	// Determine if this is a root span
	// Log events are never root spans
	switch {
	case p.MetaSignalType == "log":
		p.MetaRefineryRoot.Set(false)
	case len(parentIdFieldNames) > 0 && !p.MetaRefineryRoot.HasValue:
		// If we found a parent ID, it's not a root span
		p.MetaRefineryRoot.Set(!parentIdFound || parentIdValue == "")
	}
}

// NewPayload creates a new Payload from a map of fields. This is not populate
// metadata fields; to do this, you MUST call ExtractMetadata.
func NewPayload(data map[string]any) Payload {
	p := Payload{
		memoizedFields: data,
	}
	return p
}

// UnmarshalMsgpack implements msgpack.Unmarshaler, but doesn't unmarshal. Instead it
// keep a reference to serialized data.
func (p *Payload) UnmarshalMsgpack(data []byte) error {
	p.msgpMap = MsgpPayloadMap{rawData: data}
	return nil
}

// UnmarshalMsg implements msgp.Unmarshaler, similar to above but expects to be
// part of a larger message.
func (p *Payload) UnmarshalMsg(bts []byte) (o []byte, err error) {
	// Oddly the msgp library doesn't export the internal size method it uses
	// to skip data. So we will derived it from the returned slice.
	remainder, err := msgp.Skip(bts)
	if err != nil {
		return nil, err
	}
	ourData := bts[:len(bts)-len(remainder)]
	p.msgpMap = MsgpPayloadMap{rawData: ourData}
	return remainder, err
}

func (p *Payload) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := jsoniter.Unmarshal(data, &fields); err != nil {
		return err
	}
	p.memoizedFields = fields
	return nil
}

func (p *Payload) MemoizeFields(keys ...string) {
	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any, len(keys))
	}
	if p.missingFields == nil {
		p.missingFields = make(map[string]struct{}, len(keys))
	}

	keysToFind := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, ok := p.missingFields[key]; ok {
			continue
		}
		if _, ok := p.memoizedFields[key]; !ok {
			keysToFind[key] = struct{}{}
		}
	}
	if len(keysToFind) == 0 {
		return
	}

	iter, err := p.msgpMap.Iterate()
	if err != nil {
		return
	}

	var keysFound int
	for keysFound < len(keysToFind) {
		keyBytes, _, err := iter.NextKey()
		if err != nil {
			break
		}

		// Note we deliberately don't put string(keyBytes) in a variable here,
		// because doing so will move it to the heap on every iteration.
		// Keeping the string cast inline like this allows us to avoid the heap
		// unless we're actually going to memoize the field.
		if _, ok := keysToFind[string(keyBytes)]; ok {
			value, err := iter.ValueAny()
			if err != nil {
				break
			}
			key := string(keyBytes)
			p.memoizedFields[key] = value
			keysFound++
		}
	}

	for key := range keysToFind {
		if _, ok := p.memoizedFields[key]; !ok {
			p.missingFields[key] = struct{}{}
		}
	}
}

func (p *Payload) Exists(key string) bool {
	if p.memoizedFields != nil {
		if _, ok := p.memoizedFields[key]; ok {
			return true
		}
	}

	if p.missingFields != nil {
		if _, ok := p.missingFields[key]; ok {
			return false
		}
	}

	iter, err := p.msgpMap.Iterate()
	if err != nil {
		return false
	}

	for {
		keyBytes, _, err := iter.NextKey()
		if err != nil {
			break
		}

		if string(keyBytes) == key {
			return true
		}
	}

	return false
}

// Get retrieves a value from the Payload by key.
// Use Get if the field is expected to only be accessed once.
// If the field is expected to be accessed multiple times, use MemoizeFields
func (p *Payload) Get(key string) any {
	// Check if this is a metadata field and return from dedicated field
	switch key {
	case MetaSignalType:
		return p.MetaSignalType
	case MetaTraceID:
		return p.MetaTraceID
	case MetaAnnotationType:
		return p.MetaAnnotationType
	case MetaRefineryProbe:
		if p.MetaRefineryProbe.HasValue {
			return p.MetaRefineryProbe.Value
		}
		return nil
	case MetaRefineryRoot:
		if p.MetaRefineryRoot.HasValue {
			return p.MetaRefineryRoot.Value
		}
		return nil
	case MetaRefineryIncomingUserAgent:
		return p.MetaRefineryIncomingUserAgent
	case MetaRefinerySendBy:
		return p.MetaRefinerySendBy
	case MetaRefinerySpanDataSize:
		return p.MetaRefinerySpanDataSize
	case MetaRefineryMinSpan:
		if p.MetaRefineryMinSpan.HasValue {
			return p.MetaRefineryMinSpan.Value
		}
		return nil
	case MetaRefineryForwarded:
		return p.MetaRefineryForwarded
	case MetaRefineryExpiredTrace:
		if p.MetaRefineryExpiredTrace.HasValue {
			return p.MetaRefineryExpiredTrace.Value
		}
		return nil
	}

	if p.memoizedFields != nil {
		if value, ok := p.memoizedFields[key]; ok {
			return value
		}
	}

	if p.missingFields != nil {
		if _, ok := p.missingFields[key]; ok {
			return nil
		}
	}

	iter, err := p.msgpMap.Iterate()
	if err != nil {
		return nil
	}

	for {
		keyBytes, _, err := iter.NextKey()
		if err != nil {
			break
		}

		if string(keyBytes) == key {
			value, err := iter.ValueAny()
			if err == nil {
				return value
			}
			break
		}
	}

	return nil
}

func (p *Payload) Set(key string, value any) {
	// Check if this is a metadata field and update dedicated field
	switch key {
	case MetaSignalType:
		if v, ok := value.(string); ok {
			p.MetaSignalType = v
		}
	case MetaTraceID:
		if v, ok := value.(string); ok {
			p.MetaTraceID = v
		}
	case MetaAnnotationType:
		if v, ok := value.(string); ok {
			p.MetaAnnotationType = v
		}
	case MetaRefineryProbe:
		if v, ok := value.(bool); ok {
			p.MetaRefineryProbe.Set(v)
		}
	case MetaRefineryRoot:
		if v, ok := value.(bool); ok {
			p.MetaRefineryRoot.Set(v)
		}
	case MetaRefineryIncomingUserAgent:
		if v, ok := value.(string); ok {
			p.MetaRefineryIncomingUserAgent = v
		}
	case MetaRefinerySendBy:
		if v, ok := value.(int64); ok {
			p.MetaRefinerySendBy = v
		}
	case MetaRefinerySpanDataSize:
		if v, ok := value.(int64); ok {
			p.MetaRefinerySpanDataSize = v
		}
	case MetaRefineryMinSpan:
		if v, ok := value.(bool); ok {
			p.MetaRefineryMinSpan.Set(v)
		}
	case MetaRefineryForwarded:
		if v, ok := value.(string); ok {
			p.MetaRefineryForwarded = v
		}
	case MetaRefineryExpiredTrace:
		if v, ok := value.(bool); ok {
			p.MetaRefineryExpiredTrace.Set(v)
		}
	}

	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any)
	}
	p.memoizedFields[key] = value
}

func (p *Payload) IsEmpty() bool {
	return len(p.memoizedFields) == 0 && p.msgpMap.Size() == 0
}

// All() allows easily iterating all values in the Payload, but this is very
// NOT EFFICIENT relative to getting a subset of values using Get. Don't use
// this in non-test code unless you have to other choice.
// We only expect this to happen when transmitting sampled events using Libhoney.
func (p *Payload) All() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		// First yield metadata fields with non-default values
		if p.MetaSignalType != "" {
			if !yield(MetaSignalType, p.MetaSignalType) {
				return
			}
		}
		if p.MetaTraceID != "" {
			if !yield(MetaTraceID, p.MetaTraceID) {
				return
			}
		}
		if p.MetaAnnotationType != "" {
			if !yield(MetaAnnotationType, p.MetaAnnotationType) {
				return
			}
		}
		// Only yield boolean fields if they were explicitly set
		if p.MetaRefineryProbe.HasValue {
			if !yield(MetaRefineryProbe, p.MetaRefineryProbe.Value) {
				return
			}
		}
		if p.MetaRefineryRoot.HasValue {
			if !yield(MetaRefineryRoot, p.MetaRefineryRoot.Value) {
				return
			}
		}
		if p.MetaRefineryIncomingUserAgent != "" {
			if !yield(MetaRefineryIncomingUserAgent, p.MetaRefineryIncomingUserAgent) {
				return
			}
		}
		if p.MetaRefinerySendBy != 0 {
			if !yield(MetaRefinerySendBy, p.MetaRefinerySendBy) {
				return
			}
		}
		if p.MetaRefinerySpanDataSize != 0 {
			if !yield(MetaRefinerySpanDataSize, p.MetaRefinerySpanDataSize) {
				return
			}
		}
		if p.MetaRefineryMinSpan.HasValue {
			if !yield(MetaRefineryMinSpan, p.MetaRefineryMinSpan.Value) {
				return
			}
		}
		if p.MetaRefineryForwarded != "" {
			if !yield(MetaRefineryForwarded, p.MetaRefineryForwarded) {
				return
			}
		}
		if p.MetaRefineryExpiredTrace.HasValue {
			if !yield(MetaRefineryExpiredTrace, p.MetaRefineryExpiredTrace.Value) {
				return
			}
		}

		// Then yield memoized fields
		for key, value := range p.memoizedFields {
			if !yield(key, value) {
				return
			}
		}

		// Then iterate through msgpMap for any remaining fields
		iter, err := p.msgpMap.Iterate()
		if err != nil {
			return
		}

		for {
			keyBytes, _, err := iter.NextKey()
			if err != nil {
				break
			}

			key := string(keyBytes)
			if p.memoizedFields != nil {
				if _, ok := p.memoizedFields[key]; ok {
					// Don't yield the same key twice. Memoized values take
					// precedence over serialized values, which we can't
					// update.
					continue
				}
			}

			// Skip metadata fields as they're already yielded
			switch key {
			case MetaSignalType, MetaTraceID, MetaAnnotationType,
				MetaRefineryProbe, MetaRefineryRoot, MetaRefineryIncomingUserAgent,
				MetaRefinerySendBy, MetaRefinerySpanDataSize, MetaRefineryMinSpan,
				MetaRefineryForwarded, MetaRefineryExpiredTrace:
				continue
			}

			value, err := iter.ValueAny()
			if err != nil {
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

// Estimates data size, not very accurately, but it's fast.
func (p *Payload) GetDataSize() int {
	total := p.msgpMap.Size()
	for k, v := range p.memoizedFields {
		total += len(k) + getByteSize(v)
	}
	return total
}

// getByteSize returns the size of the given value in bytes.
// This is a rough estimate, but it's good enough for our purposes.
// Maps and slices are the most complex, so we'll just add up the sizes of their entries.
func getByteSize(val any) int {
	switch value := val.(type) {
	case bool:
		return 1
	case float64, int64, int:
		return 8
	case string:
		return len(value)
	case []byte: // also catch []uint8
		return len(value)
	case []any:
		total := 0
		for _, v := range value {
			total += getByteSize(v)
		}
		return total
	case map[string]any:
		total := 0
		for k, v := range value {
			total += len(k) + getByteSize(v)
		}
		return total
	default:
		return 8 // catchall
	}
}

// MarshalJSON implements json.Marshaler to serialize the Payload as a single JSON object
// containing all fields from both memoizedFields and msgpMap. This is incredibly
// inefficient and is only here (for now) to support our legacy nested field implementation.
func (p Payload) MarshalJSON() ([]byte, error) {
	return json.Marshal(maps.Collect(p.All()))
}

// Implements msgpack.Marshaler.
// Inefficient, only here for test cases where we serialize a Payload field.
func (p Payload) MarshalMsgpack() ([]byte, error) {
	return p.MarshalMsg(nil)
}

// Implements msgp.Marshaler.
// Appends marshaled payload to supplied buffer.
func (p Payload) MarshalMsg(buf []byte) ([]byte, error) {
	// Save the starting length of the buffer
	startLen := len(buf)

	// Reserve space for map16 header (always 3 bytes: 0xde + 2 byte count)
	// Note that for <16 elements, a single-byte header could be used instead,
	// but to take advantage of that we'd have to do an expensive copy further
	// down. So, we'll just use the 3-byte header in all cases.
	buf = append(buf, 0xde, 0, 0)

	var actualCount uint32

	// Serialize metadata fields with non-default values
	if p.MetaSignalType != "" {
		buf = msgp.AppendString(buf, MetaSignalType)
		buf = msgp.AppendString(buf, p.MetaSignalType)
		actualCount++
	}
	if p.MetaTraceID != "" {
		buf = msgp.AppendString(buf, MetaTraceID)
		buf = msgp.AppendString(buf, p.MetaTraceID)
		actualCount++
	}
	if p.MetaAnnotationType != "" {
		buf = msgp.AppendString(buf, MetaAnnotationType)
		buf = msgp.AppendString(buf, p.MetaAnnotationType)
		actualCount++
	}
	// Only serialize boolean fields if they were explicitly set
	if p.MetaRefineryProbe.HasValue {
		buf = msgp.AppendString(buf, MetaRefineryProbe)
		buf = msgp.AppendBool(buf, p.MetaRefineryProbe.Value)
		actualCount++
	}
	if p.MetaRefineryRoot.HasValue {
		buf = msgp.AppendString(buf, MetaRefineryRoot)
		buf = msgp.AppendBool(buf, p.MetaRefineryRoot.Value)
		actualCount++
	}
	if p.MetaRefineryIncomingUserAgent != "" {
		buf = msgp.AppendString(buf, MetaRefineryIncomingUserAgent)
		buf = msgp.AppendString(buf, p.MetaRefineryIncomingUserAgent)
		actualCount++
	}
	if p.MetaRefinerySendBy != 0 {
		buf = msgp.AppendString(buf, MetaRefinerySendBy)
		buf = msgp.AppendInt64(buf, p.MetaRefinerySendBy)
		actualCount++
	}
	if p.MetaRefinerySpanDataSize != 0 {
		buf = msgp.AppendString(buf, MetaRefinerySpanDataSize)
		buf = msgp.AppendInt64(buf, p.MetaRefinerySpanDataSize)
		actualCount++
	}
	if p.MetaRefineryMinSpan.HasValue {
		buf = msgp.AppendString(buf, MetaRefineryMinSpan)
		buf = msgp.AppendBool(buf, p.MetaRefineryMinSpan.Value)
		actualCount++
	}
	if p.MetaRefineryForwarded != "" {
		buf = msgp.AppendString(buf, MetaRefineryForwarded)
		buf = msgp.AppendString(buf, p.MetaRefineryForwarded)
		actualCount++
	}
	if p.MetaRefineryExpiredTrace.HasValue {
		buf = msgp.AppendString(buf, MetaRefineryExpiredTrace)
		buf = msgp.AppendBool(buf, p.MetaRefineryExpiredTrace.Value)
		actualCount++
	}

	// Serialize regular memoized fields
	for key, value := range p.memoizedFields {
		buf = msgp.AppendString(buf, key)
		var err error
		buf, err = msgp.AppendIntf(buf, value)
		if err != nil {
			return buf, err
		}
		actualCount++
	}

	// Serialize msgpMap fields, skipping duplicates
	iter, err := p.msgpMap.Iterate()
	if err == nil {
		for {
			keyBytes, _, err := iter.NextKey()
			if err != nil {
				break
			}

			keyStr := string(keyBytes)
			// Skip if already serialized from memoizedFields
			if _, ok := p.memoizedFields[keyStr]; ok {
				continue
			}

			// Skip metadata fields as they're serialized separately
			switch keyStr {
			case MetaSignalType, MetaTraceID, MetaAnnotationType,
				MetaRefineryProbe, MetaRefineryRoot, MetaRefineryIncomingUserAgent,
				MetaRefinerySendBy, MetaRefinerySpanDataSize, MetaRefineryMinSpan,
				MetaRefineryForwarded, MetaRefineryExpiredTrace:
				continue
			}

			raw, err := iter.valueSerializedBytesZC()
			if err != nil {
				return buf, err
			}

			// Why AppendStringFromBytes? Because maps keys _can_ be a binary
			// type, but msgp expects them to be a string type. The fallback to
			// the binary read in ReadMapKeyZC which we use to read these
			// allocates garbage memory.
			buf = msgp.AppendStringFromBytes(buf, keyBytes)
			buf = append(buf, raw...)
			actualCount++
		}
	}

	// Check that we don't exceed the map16 limit
	if actualCount > 65535 {
		return buf, fmt.Errorf("payload has %d fields, exceeds msgpack map16 limit of 65535", actualCount)
	}

	// Write the actual count into the reserved bytes (big-endian)
	buf[startLen+1] = byte(actualCount >> 8)
	buf[startLen+2] = byte(actualCount)

	return buf, nil
}

// TODO implement Sizer so buffer can be correctly presized

// For debugging purposes
func (p Payload) String() string {
	buf, _ := p.MarshalJSON()
	return string(buf)
}

// When trying to find a particular []byte in a slice of strings, we could use
// slices.Contains, but this involves casting the []byte to a string which does
// a heap allocation. This is cheaper.
func sliceContains(in []string, find []byte) bool {
	for i := range in {
		if bytes.Equal([]byte(in[i]), find) {
			return true
		}
	}
	return false
}
