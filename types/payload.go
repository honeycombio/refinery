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
	MetaAnnotationType            any          // meta.annotation_type (can be string or int64)
	MetaRefineryProbe             nullableBool // meta.refinery.probe
	MetaRefineryRoot              nullableBool // meta.refinery.root
	MetaRefineryIncomingUserAgent string       // meta.refinery.incoming_user_agent
	MetaRefinerySendBy            int64        // meta.refinery.send_by (Unix timestamp)
	MetaRefinerySpanDataSize      int64        // meta.refinery.span_data_size
	MetaRefineryMinSpan           nullableBool // meta.refinery.min_span
	MetaRefineryForwarded         string       // meta.refinery.forwarded
	MetaRefineryExpiredTrace      nullableBool // meta.refinery.expired_trace
}

// HasExtractedMetadata returns true if metadata has already been extracted
func (p *Payload) HasExtractedMetadata() bool {
	// We consider metadata extracted if:
	// 1. We have a trace ID (for trace events)
	// 2. We have a signal type set (for any event type)
	// 3. Both msgpMap and memoizedFields are empty (empty payload)
	// Note: We can't just check msgpMap.Size() == 0 because JSON payloads won't have msgpMap data
	return p.MetaTraceID != "" || p.MetaSignalType != "" || (p.msgpMap.Size() == 0 && len(p.memoizedFields) == 0)
}

// metadataExtractor is a helper type to track parent ID information during extraction
type metadataExtractor struct {
	parentIdFound bool
	parentIdValue string
}

// extractMetadataFromBytes extracts metadata from msgpack data.
// If consumed is non-nil, it will be set to the number of bytes consumed from the data.
// The parentIdInfo is used to track parent ID state across calls.
func (p *Payload) extractMetadataFromBytes(data []byte, traceIdFieldNames, parentIdFieldNames []string, consumed *int, parentIdInfo *metadataExtractor) error {
	if parentIdInfo == nil {
		parentIdInfo = &metadataExtractor{}
	}

	// Read the map header
	mapSize, remaining, err := msgp.ReadMapHeaderBytes(data)
	if err != nil {
		return fmt.Errorf("failed to read msgpack map header: %w", err)
	}

	if consumed != nil {
		*consumed = len(data) - len(remaining)
	}

	// Process all map entries
	for i := uint32(0); i < mapSize; i++ {
		// Read the key
		var keyBytes []byte
		keyBytes, remaining, err = msgp.ReadMapKeyZC(remaining)
		if err != nil {
			return fmt.Errorf("failed to read msgpack key: %w", err)
		}

		// Determine the value type
		valueType := msgp.NextType(remaining)

		// Check if this is a metadata field we care about
		handled := false

		// Handle string metadata fields
		if valueType == msgp.StrType {
			if bytes.HasPrefix(keyBytes, []byte("meta.")) {
				switch {
				case bytes.Equal(keyBytes, []byte(MetaSignalType)):
					p.MetaSignalType, remaining, err = msgp.ReadStringBytes(remaining)
					handled = true
				case bytes.Equal(keyBytes, []byte(MetaTraceID)):
					p.MetaTraceID, remaining, err = msgp.ReadStringBytes(remaining)
					handled = true
				case bytes.Equal(keyBytes, []byte(MetaAnnotationType)):
					p.MetaAnnotationType, remaining, err = msgp.ReadStringBytes(remaining)
					handled = true
				case bytes.Equal(keyBytes, []byte(MetaRefineryIncomingUserAgent)):
					p.MetaRefineryIncomingUserAgent, remaining, err = msgp.ReadStringBytes(remaining)
					handled = true
				case bytes.Equal(keyBytes, []byte(MetaRefineryForwarded)):
					p.MetaRefineryForwarded, remaining, err = msgp.ReadStringBytes(remaining)
					handled = true
				}
			} else if p.MetaTraceID == "" && sliceContains(traceIdFieldNames, keyBytes) {
				p.MetaTraceID, remaining, err = msgp.ReadStringBytes(remaining)
				handled = true
			} else if !parentIdInfo.parentIdFound && sliceContains(parentIdFieldNames, keyBytes) {
				var parentId string
				parentId, remaining, err = msgp.ReadStringBytes(remaining)
				if err == nil && parentId != "" {
					parentIdInfo.parentIdFound = true
					parentIdInfo.parentIdValue = parentId
				}
				handled = true
			}
		} else if valueType == msgp.BoolType {
			// Handle boolean metadata fields
			switch {
			case bytes.Equal(keyBytes, []byte(MetaRefineryMinSpan)):
				var val bool
				val, remaining, err = msgp.ReadBoolBytes(remaining)
				if err == nil {
					p.MetaRefineryMinSpan.Set(val)
				}
				handled = true
			case bytes.Equal(keyBytes, []byte(MetaRefineryExpiredTrace)):
				var val bool
				val, remaining, err = msgp.ReadBoolBytes(remaining)
				if err == nil {
					p.MetaRefineryExpiredTrace.Set(val)
				}
				handled = true
			case bytes.Equal(keyBytes, []byte(MetaRefineryProbe)):
				var val bool
				val, remaining, err = msgp.ReadBoolBytes(remaining)
				if err == nil {
					p.MetaRefineryProbe.Set(val)
				}
				handled = true
			case bytes.Equal(keyBytes, []byte(MetaRefineryRoot)):
				var val bool
				val, remaining, err = msgp.ReadBoolBytes(remaining)
				if err == nil {
					p.MetaRefineryRoot.Set(val)
				}
				handled = true
			}
		} else if valueType == msgp.IntType || valueType == msgp.UintType {
			// Handle int64 metadata fields
			switch {
			case bytes.Equal(keyBytes, []byte(MetaAnnotationType)):
				p.MetaAnnotationType, remaining, err = msgp.ReadInt64Bytes(remaining)
				handled = true
			case bytes.Equal(keyBytes, []byte(MetaRefinerySendBy)):
				p.MetaRefinerySendBy, remaining, err = msgp.ReadInt64Bytes(remaining)
				handled = true
			case bytes.Equal(keyBytes, []byte(MetaRefinerySpanDataSize)):
				p.MetaRefinerySpanDataSize, remaining, err = msgp.ReadInt64Bytes(remaining)
				handled = true
			}
		}

		if err != nil {
			return fmt.Errorf("failed to read value for key %s: %w", string(keyBytes), err)
		}

		// If we didn't handle this field as metadata, skip it
		if !handled {
			remaining, err = msgp.Skip(remaining)
			if err != nil {
				return fmt.Errorf("failed to skip value: %w", err)
			}
		}

		if consumed != nil {
			*consumed = len(data) - len(remaining)
		}
	}

	// Determine if this is a root span
	switch {
	case p.MetaSignalType == "log":
		p.MetaRefineryRoot.Set(false)
	case len(parentIdFieldNames) > 0 && !p.MetaRefineryRoot.HasValue:
		// If we found a parent ID, it's not a root span
		p.MetaRefineryRoot.Set(!parentIdInfo.parentIdFound || parentIdInfo.parentIdValue == "")
	}

	return nil
}

// ExtractMetadata populates the cached metadata fields from the payload data.
// This MUST be called manually after creating or unmarshaling a non-empty Payload
// to populate the metadata fields. The traceIdFieldNames and parentIdFieldNames parameters
// are optional and used to extract trace ID and determine if the span is a root span.
func (p *Payload) ExtractMetadata(traceIdFieldNames, parentIdFieldNames []string) error {
	parentIdInfo := &metadataExtractor{}

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
				p.MetaAnnotationType = value
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
				if !parentIdInfo.parentIdFound {
					for _, field := range parentIdFieldNames {
						if key == field {
							if v, ok := value.(string); ok && v != "" {
								parentIdInfo.parentIdFound = true
								parentIdInfo.parentIdValue = v
							}
							break
						}
					}
				}
			}
		}
	}

	// For msgpMap fields, extract from the raw bytes
	if p.msgpMap.Size() > 0 {
		return p.extractMetadataFromBytes(p.msgpMap.rawData, traceIdFieldNames, parentIdFieldNames, nil, parentIdInfo)
	}

	// Handle root span detection for memoized-only payloads
	switch {
	case p.MetaSignalType == "log":
		p.MetaRefineryRoot.Set(false)
	case len(parentIdFieldNames) > 0 && !p.MetaRefineryRoot.HasValue:
		// If we found a parent ID, it's not a root span
		p.MetaRefineryRoot.Set(!parentIdInfo.parentIdFound || parentIdInfo.parentIdValue == "")
	}

	return nil
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

// UnmarshalMsgWithMetadata unmarshals the payload and extracts metadata in a single pass.
// This is more efficient than calling UnmarshalMsg followed by ExtractMetadata separately.
// It returns the remaining bytes after unmarshaling.
func (p *Payload) UnmarshalMsgWithMetadata(bts []byte, traceIdFieldNames, parentIdFieldNames []string) (o []byte, err error) {
	// Extract metadata and get consumed bytes
	var consumed int
	err = p.extractMetadataFromBytes(bts, traceIdFieldNames, parentIdFieldNames, &consumed, nil)
	if err != nil {
		return nil, err
	}

	// Store the raw data
	ourData := bts[:consumed]
	p.msgpMap = MsgpPayloadMap{rawData: ourData}

	// Return remainder
	return bts[consumed:], nil
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
		p.MetaAnnotationType = value
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
		if p.MetaAnnotationType != nil {
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
	if p.MetaAnnotationType != nil {
		buf = msgp.AppendString(buf, MetaAnnotationType)
		switch v := p.MetaAnnotationType.(type) {
		case string:
			buf = msgp.AppendString(buf, v)
		case int64:
			buf = msgp.AppendInt64(buf, v)
		default:
			// This shouldn't happen, but handle it gracefully
			buf = msgp.AppendString(buf, fmt.Sprintf("%v", v))
		}
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
