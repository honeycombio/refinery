package config

import (
	"time"
)

const (
	DryRunFieldName = "meta.refinery.dryrun.kept"
)

// Config defines the interface the rest of the code uses to get items from the
// config. There are different implementations of the config using different
// backends to store the config. FileConfig is the default and uses a
// TOML-formatted config file. RedisPeerFileConfig uses a redis cluster to store
// the list of peers and then falls back to a filesystem config file for all
// other config elements.

type Config interface {
	// RegisterReloadCallback takes a name and a function that will be called
	// when the configuration is reloaded. This will happen infrequently. If
	// consumers of configuration set config values on startup, they should
	// check their values haven't changed and re-start anything that needs
	// restarting with the new values.
	RegisterReloadCallback(callback func())

	// GetListenAddr returns the address and port on which to listen for
	// incoming events
	GetListenAddr() (string, error)

	// GetPeerListenAddr returns the address and port on which to listen for
	// peer traffic
	GetPeerListenAddr() (string, error)

	// GetCompressPeerCommunication will be true if refinery should compress
	// data before forwarding it to a peer.
	GetCompressPeerCommunication() bool

	// GetGRPCEnabled returns or not the GRPC server is enabled.
	GetGRPCEnabled() bool

	// GetGRPCListenAddr returns the address and port on which to listen for
	// incoming events over gRPC
	GetGRPCListenAddr() (string, error)

	// IsAPIKeyValid checks if the given API key is valid according to the rules
	IsAPIKeyValid(key string) bool

	// GetPeers returns a list of other servers participating in this proxy cluster
	GetPeers() ([]string, error)

	GetPeerManagementType() (string, error)

	// GetRedisHost returns the address of a Redis instance to use for peer
	// management.
	GetRedisHost() (string, error)

	// GetRedisUsername returns the username of a Redis instance to use for peer
	// management.
	GetRedisUsername() (string, error)

	// GetRedisPassword returns the password of a Redis instance to use for peer
	// management.
	GetRedisPassword() (string, error)

	// GetRedisPrefix returns the prefix string used in the keys for peer
	// management.
	GetRedisPrefix() string

	// GetRedisDatabase returns the ID of the Redis database to use for peer management.
	GetRedisDatabase() int

	// GetUseTLS returns true when TLS must be enabled to dial the Redis instance to
	// use for peer management.
	GetUseTLS() (bool, error)

	// UseTLSInsecure returns true when certificate checks are disabled
	GetUseTLSInsecure() (bool, error)

	// GetHoneycombAPI returns the base URL (protocol, hostname, and port) of
	// the upstream Honeycomb API server
	GetHoneycombAPI() (string, error)

	// GetSendDelay returns the number of seconds to pause after a trace is
	// complete before sending it, to allow stragglers to arrive
	GetSendDelay() (time.Duration, error)

	// GetBatchTimeout returns how often to send off batches in seconds
	GetBatchTimeout() time.Duration

	// GetTraceTimeout is how long to wait before sending a trace even if it's
	// not complete. This should be longer than the longest expected trace
	// duration.
	GetTraceTimeout() (time.Duration, error)

	// GetMaxBatchSize is the number of events to be included in the batch for sending
	GetMaxBatchSize() uint

	// GetLoggerType returns the type of the logger to use. Valid types are in
	// the logger package
	GetLoggerType() (string, error)

	// GetLoggerLevel returns the level of the logger to use.
	GetLoggerLevel() Level

	// GetHoneycombLoggerConfig returns the config specific to the HoneycombLogger
	GetHoneycombLoggerConfig() (HoneycombLoggerConfig, error)

	// GetCollectionConfig returns the config specific to the InMemCollector
	GetCollectionConfig() (CollectionConfig, error)

	// GetSamplerConfigForDestName returns the sampler type and name to use for
	// the given destination (environment, or dataset in classic)
	GetSamplerConfigForDestName(string) (interface{}, string, error)

	// GetAllSamplerRules returns all rules in a single map, including the default rules
	GetAllSamplerRules() (*V2SamplerConfig, error)

	// GetLegacyMetricsConfig returns the config specific to LegacyMetrics
	GetLegacyMetricsConfig() LegacyMetricsConfig

	// GetPrometheusMetricsConfig returns the config specific to PrometheusMetrics
	GetPrometheusMetricsConfig() PrometheusMetricsConfig

	// GetOTelMetricsConfig returns the config specific to OTelMetrics
	GetOTelMetricsConfig() OTelMetricsConfig

	// GetUpstreamBufferSize returns the size of the libhoney buffer to use for the upstream
	// libhoney client
	GetUpstreamBufferSize() int
	// GetPeerBufferSize returns the size of the libhoney buffer to use for the peer forwarding
	// libhoney client
	GetPeerBufferSize() int

	GetIdentifierInterfaceName() (string, error)

	GetUseIPV6Identifier() (bool, error)

	GetRedisIdentifier() (string, error)

	// GetSendTickerValue returns the duration to use to check for traces to send
	GetSendTickerValue() time.Duration

	// GetDebugServiceAddr sets the IP and port the debug service will run on (you must provide the
	// command line flag -d to start the debug service)
	GetDebugServiceAddr() (string, error)

	GetIsDryRun() bool

	GetAddHostMetadataToTrace() bool

	GetAddRuleReasonToTrace() bool

	GetEnvironmentCacheTTL() time.Duration

	GetDatasetPrefix() string

	// GetQueryAuthToken returns the token that must be used to access the /query endpoints
	GetQueryAuthToken() string

	GetGRPCMaxConnectionIdle() time.Duration

	GetGRPCMaxConnectionAge() time.Duration

	GetGRPCMaxConnectionAgeGrace() time.Duration

	GetGRPCKeepAlive() time.Duration

	GetGRPCKeepAliveTimeout() time.Duration

	GetPeerTimeout() time.Duration

	GetAdditionalErrorFields() []string

	GetAddSpanCountToRoot() bool

	GetConfigMetadata() []ConfigMetadata

	GetSampleCacheConfig() SampleCacheConfig

	GetStressReliefConfig() StressReliefConfig

	GetAdditionalAttributes() map[string]string

	GetTraceIdFieldNames() []string

	GetParentIdFieldNames() []string
}

type ConfigMetadata struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	Hash     string `json:"hash"`
	LoadedAt string `json:"loaded_at"`
}
