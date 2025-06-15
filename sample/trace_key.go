package sample

import (
	"bytes"
	"fmt"
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
	keyBuilder     *bytes.Buffer
	distinctValue  *distinctValue
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
		distinctValue:  &distinctValue{buf: make([]byte, 0, 1024)},
		keyBuilder:     &bytes.Buffer{},
	}
}

// build, builds the trace key based on the configuration of the traceKeyGenerator
// returns the number of values used to build the key
func (d *traceKey) build(trace *types.Trace) (string, int) {
	fieldCount := 0

	// for each field, for each span, get the value of that field
	spans := trace.GetSpans()
	d.distinctValue.Reset(d.fields, maxKeyLength)
	d.keyBuilder.Reset()
outer:
	for i, field := range d.fields {
		for _, span := range spans {
			if val, ok := span.Data[field]; ok {
				if d.distinctValue.totalUniqueCount >= maxKeyLength {
					break outer
				}
				d.distinctValue.AddAsString(val, i)
			}
		}
	}
	// ok, now we have a map of fields to a list of all unique values for that field.
	// (unless it was huge, in which case we have a bunch of them)

	// please change this so that strings.Builder can be reused between calls

	for i := range d.fields {
		values := d.distinctValue.Values(i)
		// if there's no values for this field, skip it
		if len(values) == 0 {
			continue
		}
		var prevStr string
		for _, str := range values {
			if str != prevStr {
				d.keyBuilder.WriteString(str)
				d.keyBuilder.WriteRune('•')
				fieldCount += 1
			}
			prevStr = str
		}
		// get ready for the next element
		d.keyBuilder.WriteRune(',')
	}

	if trace.RootSpan != nil {
		for _, field := range d.rootOnlyFields {
			if val, ok := trace.RootSpan.Data[field]; ok {
				d.keyBuilder.WriteString(fmt.Sprintf("%v,", val))
				fieldCount += 1
			}
		}
	}

	if d.useTraceLength {
		d.keyBuilder.WriteString(strconv.FormatInt(int64(len(spans)), 10))
		fieldCount += 1
	}

	return d.keyBuilder.String(), fieldCount
}

// distinctValue keeps track of distinct values for a set of fields.
// It stores the unique values as strings.
type distinctValue struct {
	buf          []byte
	values       []map[uint64]string
	valuesBuffer []string

	// totalUniqueCount keeps track of how many unique values we've seen so far for a trace key.
	totalUniqueCount int
	// maxDistinctValue is the maximum number of distinct values we will store for a trace key.
	maxDistinctValue int
}

func (d *distinctValue) Reset(fields []string, maxDistinctValue int) {
	// Reset the fields and values but do not reallocate them
	d.maxDistinctValue = maxDistinctValue

	for len(d.values) < len(fields) {
		d.values = append(d.values, make(map[uint64]string))
	}

	d.values = d.values[:len(fields)]
	for i := range d.values {
		clear(d.values[i])
	}

	d.totalUniqueCount = 0
	d.buf = d.buf[:0]
	d.valuesBuffer = d.valuesBuffer[:0]
}

// Values returns the distinct values for a given field index.
// It returns a sorted slice of strings containing the unique values for that field.
func (d *distinctValue) Values(fieldIdx int) []string {
	if fieldIdx < 0 || fieldIdx >= len(d.values) {
		return nil
	}

	valueMap := d.values[fieldIdx]
	if len(valueMap) == 0 {
		return nil
	}

	// reuse the valuesBuffer to avoid allocations
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
	d.buf = d.buf[:0] // reset the buffer for each new value

	switch v := value.(type) {
	case string:
		d.buf = append(d.buf, []byte(v)...)
	case int:
		d.buf = strconv.AppendInt(d.buf, int64(v), 10)
	case int64:
		d.buf = strconv.AppendInt(d.buf, v, 10)
	case float64:
		d.buf = strconv.AppendFloat(d.buf, v, 'f', -1, 64)
	case bool:
		d.buf = strconv.AppendBool(d.buf, v)
	default:
		d.buf = append(d.buf, fmt.Sprintf("%v", v)...)
	}

	hash := metro.Hash64(d.buf, 0)
	if _, exists := d.values[fieldIdx][hash]; !exists {
		d.totalUniqueCount++
		if d.totalUniqueCount >= d.maxDistinctValue {
			return false
		}
		d.values[fieldIdx][hash] = string(d.buf)
		return true
	}

	return false
}
