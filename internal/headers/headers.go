package headers

import "strings"

// reservedPrefixes contains HTTP header prefixes that cannot be overridden
// via AdditionalHeaders because they are reserved for Honeycomb API communication.
var reservedPrefixes = []string{
	"x-honeycomb-",
	"x-hny-",
}

// IsReserved checks if a header name is a reserved Honeycomb header.
// Any header starting with "X-Honeycomb-" or "X-Hny-" is reserved.
// The check is case-insensitive.
func IsReserved(name string) bool {
	lower := strings.ToLower(name)
	for _, prefix := range reservedPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}
