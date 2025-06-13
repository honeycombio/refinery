package types

import (
	"encoding/json"
	"iter"
	"maps"

	jsoniter "github.com/json-iterator/go"
	"github.com/vmihailenco/msgpack/v5"
)

type Payload struct {
	// A serialized messagepack map used to source fields.
	msgpMap *MsgpPayloadMap

	// Deserialized fields, either from the internal msgpMap, or set externally.
	memoizedFields map[string]any
}

func NewPayload(data map[string]any) Payload {
	return Payload{
		memoizedFields: data,
	}
}

func (p *Payload) UnmarshalMsgpack(data []byte) error {
	p.msgpMap = &MsgpPayloadMap{rawData: data}
	p.memoizedFields = make(map[string]any)
	return nil
}

func (p *Payload) UnmarshalJSON(data []byte) error {
	var fields map[string]any
	if err := jsoniter.Unmarshal(data, &fields); err != nil {
		return err
	}
	p.memoizedFields = fields
	return nil
}

// Extracts all of the listed fields from the internal msgp buffer in a single
// pass, for efficient random access later.
func (p *Payload) MemoizeFields(keys ...string) {
	if p.msgpMap == nil {
		return
	}

	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any, len(keys))
	}

	keysToFind := make(map[string]struct{}, len(keys))
	for _, key := range keys {
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
}

func (p *Payload) Get(key string) any {
	if p.memoizedFields != nil {
		if value, ok := p.memoizedFields[key]; ok {
			return value
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
	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any)
	}

	p.memoizedFields[key] = value
}

func (p *Payload) IsEmpty() bool {
	return len(p.memoizedFields) == 0 && p.msgpMap == nil
}

// All() allows easily iterating all values in the Payload, but this is very
// NOT EFFICIENT relative to getting a subset of values using Get. Don't use
// this in non-test code unless you have to other choice.
func (p *Payload) All() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		// First yield memoized fields
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
	var total int
	if p.msgpMap != nil {
		total += p.msgpMap.Size()
	}
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

// Inefficient, only here for test cases where we serialize a Payload field.
func (p Payload) MarshalMsgpack() ([]byte, error) {
	return msgpack.Marshal(maps.Collect(p.All()))
}

// For debugging purposes
func (p Payload) String() string {
	buf, _ := p.MarshalJSON()
	return string(buf)
}
