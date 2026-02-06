package headers

import "strings"

// Honeycomb API header constants
const (
	APIKeyHeader = "X-Honeycomb-Team"
	// libhoney-js uses this
	APIKeyHeaderShort = "X-Hny-Team"
	DatasetHeader     = "X-Honeycomb-Dataset"
	SampleRateHeader  = "X-Honeycomb-Samplerate"
	TimestampHeader   = "X-Honeycomb-Event-Time"
	QueryTokenHeader  = "X-Honeycomb-Refinery-Query"
)

// reservedHeaders contains HTTP headers that cannot be overridden via AdditionalHeaders
// because they are set by Refinery for proper Honeycomb API communication.
var reservedHeaders = map[string]bool{
	strings.ToLower(APIKeyHeader):      true,
	strings.ToLower(APIKeyHeaderShort): true,
	strings.ToLower(DatasetHeader):     true,
	strings.ToLower(SampleRateHeader):  true,
	strings.ToLower(TimestampHeader):   true,
}

// IsReserved checks if a header name is a reserved Honeycomb header.
// The check is case-insensitive.
func IsReserved(name string) bool {
	return reservedHeaders[strings.ToLower(name)]
}
