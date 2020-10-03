package sharder

import (
	"fmt"
	"os"

	"github.com/honeycombio/refinery/config"
)

// Shard repreesents a single instance of Refinery.
type Shard interface {
	Equals(Shard) bool
	// GetAddress returns a string suitable for use in building a URL, eg
	// http://refinery-1234:8080 or https://10.2.3.4
	GetAddress() string
}

// Sharder is for determining which shard should handle a specific trace ID
type Sharder interface {
	// MyShard returns the Shard representing this process
	MyShard() Shard
	// WhichShard takes in a trace ID as input and returns the shard responsible
	// for that trace ID
	WhichShard(string) Shard
}

func GetSharderImplementation(c config.Config) Sharder {
	var sharder Sharder
	sharderType := "deterministic" // this is not yet exposed in the config
	switch sharderType {
	case "single":
		sharder = &SingleServerSharder{}
	case "deterministic":
		sharder = &DeterministicSharder{}
	default:
		fmt.Printf("unknown sharder type %s. Exiting.\n", sharderType)
		os.Exit(1)
	}
	return sharder
}
