package sample

import (
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/dgryski/go-metro"
	"github.com/honeycombio/refinery/types"
)

// once a key gets this many unique values, it's off the charts in terms of uniqueness
// so we just stop looking for more.
// This is a safety valve to prevent us from someone sending a high-cardinality field
// in a giant trace.
const maxKeyLength = 100

type traceKey struct {
	fields         []string
	rootOnlyFields []string
	useTraceLength bool
}

func newTraceKey(fields []string, useTraceLength bool) *traceKey {
	// always put the field list in sorted order for easier comparison
	sort.Strings(fields)
	rootOnlyFields := make([]string, 0, len(fields)/2)
	nonRootFields := make([]string, 0, len(fields)/2)
	for _, field := range fields {
		if strings.HasPrefix(field, RootPrefix) {
			rootOnlyFields = append(rootOnlyFields, field[len(RootPrefix):])
			continue
		}

		nonRootFields = append(nonRootFields, field)
	}

	return &traceKey{
		fields:         nonRootFields,
		rootOnlyFields: rootOnlyFields,
		useTraceLength: useTraceLength,
	}
}

// build, builds the trace key based on the configuration of the traceKeyGenerator
// returns the number of values used to build the key
func (d *traceKey) build(trace *types.Trace) (string, int) {
	fieldCount := 0

	// for each field, for each span, get the value of that field
	spans := trace.GetSpans()
	uniques := newDistinctValue(d.fields, maxKeyLength)
outer:
	for i, field := range d.fields {
		for _, span := range spans {
			if val, ok := span.Data[field]; ok {
				// don't bother to add it if we've already seen it
				if uniques.totalUniqueCount >= maxKeyLength {
					break outer
				}
				if uniques.AddAsString(val, i) {
					continue
				}
			}
		}
	}
	// ok, now we have a map of fields to a list of all unique values for that field.
	// (unless it was huge, in which case we have a bunch of them)

	var key strings.Builder
	for i := range d.fields {
		// if there's no values for this field, skip it
		values := uniques.Values(i)
		var prevStr string
		for _, str := range values {
			if str != prevStr {
				key.WriteString(str)
				key.WriteRune('•')
				fieldCount += 1
			}
			prevStr = str
		}
		// get ready for the next element
		key.WriteRune(',')
	}

	if trace.RootSpan != nil {
		for _, field := range d.rootOnlyFields {
			if val, ok := trace.RootSpan.Data[field]; ok {
				key.WriteString(fmt.Sprintf("%v,", val))
				fieldCount += 1
			}
		}
	}

	if d.useTraceLength {
		key.WriteString(strconv.FormatInt(int64(len(spans)), 10))
		fieldCount += 1
	}

	return key.String(), fieldCount
}

// distinctValue keeps track of distinct values for a set of fields.
// It stores the unique values as strings.
type distinctValue struct {
	buf    []byte
	fields []string
	values []map[uint64]string

	// totalUniqueCount keeps track of how many unique values we've seen so far
	totalUniqueCount int
	// maxDistinctValue is the maximum number of distinct values we will store
	maxDistinctValue int
}

func newDistinctValue(fields []string, maxDistinctValue int) *distinctValue {
	d := &distinctValue{
		buf:              make([]byte, 0, 1024),
		fields:           fields,
		values:           make([]map[uint64]string, len(fields)),
		maxDistinctValue: maxDistinctValue,
	}

	for i := range d.values {
		d.values[i] = make(map[uint64]string, maxDistinctValue)
	}

	return d

}

// Values returns the distinct values for a given field index.
// It returns a sorted slice of strings containing the unique values for that field.
func (d *distinctValue) Values(fieldIdx int) []string {
	if fieldIdx < 0 || fieldIdx >= len(d.values) {
		return nil
	}

	// Get the map for the specified field index
	valueMap := d.values[fieldIdx]
	if len(valueMap) == 0 {
		return nil
	}

	values := slices.Collect(maps.Values(valueMap))
	sort.Strings(values)

	return values
}

// AddAsString adds a value to the distinct values for a given field index.
// It returns true if the value was added, false if it was already present or if the maxDistinctValue limit was reached.
func (d *distinctValue) AddAsString(value any, fieldIdx int) bool {
	if value == nil {
		return false
	}

	d.buf = d.buf[:0] // reset the buffer for each new value

	switch v := value.(type) {
	case string:
		d.buf = append(d.buf, []byte(v)...)
	case int:
		d.buf = binary.BigEndian.AppendUint64(d.buf, uint64(v))
	case int64:
		d.buf = binary.BigEndian.AppendUint64(d.buf, uint64(v))
	case float64:
		d.buf = binary.BigEndian.AppendUint64(d.buf, math.Float64bits(v))
	case bool:
		if v {
			d.buf = append(d.buf, '1')
		} else {
			d.buf = append(d.buf, '0')
		}
	default:
		d.buf = append(d.buf, fmt.Sprintf("%v", v)...)
	}

	hash := metro.Hash64(d.buf, 0)
	if _, exists := d.values[fieldIdx][hash]; !exists {
		d.totalUniqueCount++
		if d.totalUniqueCount >= d.maxDistinctValue {
			return false
		}
		d.values[fieldIdx][hash] = fmt.Sprintf("%v", value)
		return true
	}

	return false
}
