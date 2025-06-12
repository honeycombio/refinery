package sample

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

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
	uniques := distinctValuePool.Get().(*distinctValue)
	uniques.init(d.fields, maxKeyLength)
	defer func() {
		// reset the distinctValue for reuse
		uniques.Reset()
		distinctValuePool.Put(uniques)
	}()
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
		values := uniques.Values(i)
		// if there's no values for this field, skip it
		if len(values) == 0 {
			continue
		}
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

// Pool for reusing distinctValue objects.
var distinctValuePool = &sync.Pool{
	New: func() any {
		return &distinctValue{
			buf: make([]byte, 0, 1024),
		}
	},
}

// distinctValue keeps track of distinct values for a set of fields.
// It stores the unique values as strings.
type distinctValue struct {
	buf          []byte
	fields       []string
	values       []map[uint64]string
	valuesBuffer []string

	// totalUniqueCount keeps track of how many unique values we've seen so far for a trace key.
	totalUniqueCount int
	// maxDistinctValue is the maximum number of distinct values we will store for a trace key.
	maxDistinctValue int
}

func (d *distinctValue) init(fields []string, maxDistinctValue int) {
	for i := range fields {
		if i >= len(d.fields) {
			// if we don't have enough fields, allocate more
			d.fields = append(d.fields, fields[i])
			continue
		}
		d.fields[i] = fields[i]
	}

	if len(d.values) < len(fields) {
		// if we don't have enough values, allocate more
		for i := len(d.values); i < len(fields); i++ {
			d.values = append(d.values, make(map[uint64]string))
		}
	} else {
		// if we have more values than fields, trim the excess
		d.values = d.values[:len(fields)]
	}

	d.maxDistinctValue = maxDistinctValue
}

func (d *distinctValue) Reset() {
	// Reset the fields and values but do not reallocate them
	for i := range d.fields {
		d.fields[i] = ""
	}
	for i := range d.values {
		if d.values[i] != nil {
			for k := range d.values[i] {
				delete(d.values[i], k)
			}
		}
	}

	// Reset the total unique count
	d.totalUniqueCount = 0
	// Reset the buffer
	d.buf = d.buf[:0]
	// Reset the values buffer
	d.valuesBuffer = d.valuesBuffer[:0]
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

	// use the valuesBuffer to avoid unnecessary allocations
	d.valuesBuffer = d.valuesBuffer[:0]
	for _, value := range valueMap {
		d.valuesBuffer = append(d.valuesBuffer, value)
	}
	sort.Strings(d.valuesBuffer)

	return d.valuesBuffer
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
