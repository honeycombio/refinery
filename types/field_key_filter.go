package types

// fieldKeyFilter rejects keys that cannot be a configured trace ID, parent ID
// or sampling key name, so most keys of an event skip three linear name scans.
// It only reports false when no name can match, so it never changes which keys
// are extracted.
type fieldKeyFilter struct {
	lengths [4]uint64 // bit n set when some name has length n
	first   [4]uint64 // bit b set when some name starts with byte b
	// overflow is set when a name is too long for lengths; every key then
	// passes.
	overflow bool
}

func newFieldKeyFilter(nameLists ...[]string) *fieldKeyFilter {
	filter := &fieldKeyFilter{}
	for _, names := range nameLists {
		for _, name := range names {
			if len(name) == 0 || len(name) >= len(filter.lengths)*64 {
				filter.overflow = true
				continue
			}
			filter.lengths[len(name)>>6] |= 1 << (len(name) & 63)
			filter.first[name[0]>>6] |= 1 << (name[0] & 63)
		}
	}
	return filter
}

// mayMatch reports whether key could equal one of the filter's names. A nil
// filter passes every key.
func (f *fieldKeyFilter) mayMatch(key []byte) bool {
	if f == nil || f.overflow {
		return true
	}
	if len(key) == 0 || len(key) >= len(f.lengths)*64 {
		return false
	}
	return f.lengths[len(key)>>6]&(1<<(len(key)&63)) != 0 &&
		f.first[key[0]>>6]&(1<<(key[0]&63)) != 0
}
