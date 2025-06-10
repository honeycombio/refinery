package types

import "iter"

type PayloadHolder struct {
	// A serialized messagepack map used to source fields.
	msgpMap *MsgpPayloadMap

	// Deserialized fields, either from the internal msgpMap, or set externally.
	memoizedFields map[string]any
}

func NewPayloadHolderFromMsgP(raw []byte) PayloadHolder {
	return PayloadHolder{
		msgpMap: &MsgpPayloadMap{rawData: raw},
		memoizedFields: make(map[string]any),
	}
}

func NewPayloadHolderFromMap(data map[string]any) PayloadHolder {
	return PayloadHolder{
		memoizedFields: data,
	}
}

// Extracts all of the listed fields from the internal msgp buffer in a single
// pass, for efficient random access later.
func (p *PayloadHolder) MemoizeFields(keys ...string) {
	if p.msgpMap == nil {
		return
	}

	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any)
	}

	keysToFind := make(map[string]bool)
	for _, key := range keys {
		if _, exists := p.memoizedFields[key]; !exists {
			keysToFind[key] = true
		}
	}

	if len(keysToFind) == 0 {
		return
	}

	iter, err := p.msgpMap.Iterate()
	if err != nil {
		return
	}

	for len(keysToFind) > 0 {
		keyBytes, _, err := iter.NextKey()
		if err != nil {
			break
		}

		key := string(keyBytes)
		if keysToFind[key] {
			value, err := iter.ValueAny()
			if err == nil {
				p.memoizedFields[key] = value
				delete(keysToFind, key)
			}
		}
	}
}

func (p *PayloadHolder) Get(key string) any {
	if p.memoizedFields != nil {
		if value, exists := p.memoizedFields[key]; exists {
			return value
		}
	}

	if p.msgpMap == nil {
		return nil
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

func (p *PayloadHolder) Set(key string, value any) {
	if p.memoizedFields == nil {
		p.memoizedFields = make(map[string]any)
	}
	p.memoizedFields[key] = value
}

func (p *PayloadHolder) All() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		seen := make(map[string]bool)

		// First yield memoized fields
		if p.memoizedFields != nil {
			for key, value := range p.memoizedFields {
				if !yield(key, value) {
					return
				}
				seen[key] = true
			}
		}

		// Then iterate through msgpMap for any remaining fields
		if p.msgpMap != nil {
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
				if !seen[key] {
					value, err := iter.ValueAny()
					if err == nil {
						if !yield(key, value) {
							return
						}
					}
				}
			}
		}
	}
}
