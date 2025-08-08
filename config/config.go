package config

import (
	"slices"
	"strings"
	"time"
)

const (
	DryRunFieldName = "meta.refinery.dryrun.kept"
)

// Config defines the interface the rest of the code uses to get items from the
// config. There are different implementations of the config using different
// backends to store the config.

type Config interface {
	// RegisterReloadCallback takes a name and a function that will be called
	// whenever the configuration is reloaded. This will happen infrequently. If
	// consumers of configuration set config values on startup, they should
	// check their values haven't changed and re-start anything that needs
	// restarting with the new values. The callback is passed the two hashes
	// for config and rules so that the caller can decide if they need to
	// reconfigure anything.
	RegisterReloadCallback(callback ConfigReloadCallback)

	// Reload forces the config to attempt to reload its values. If the config
	// checksum has changed, the reload callbacks will be called.
	Reload(opts ...ReloadedConfigDataOption) error

	// GetHashes returns the current config and rule hashes
	GetHashes() (cfg string, rules string)

	// GetListenAddr returns the address and port on which to listen for
	// incoming events
	GetListenAddr() string

	// GetPeerListenAddr returns the address and port on which to listen for
	// peer traffic
	GetPeerListenAddr() string

	// GetHTTPIdleTimeout returns the idle timeout for refinery's HTTP server
	GetHTTPIdleTimeout() time.Duration

	// GetCompressPeerCommunication will be true if refinery should compress
	// data before forwarding it to a peer.
	GetCompressPeerCommunication() bool

	// GetGRPCEnabled returns or not the GRPC server is enabled.
	GetGRPCEnabled() bool

	// GetGRPCListenAddr returns the address and port on which to listen for
	// incoming events over gRPC
	GetGRPCListenAddr() string

	// Returns the entire GRPC config block
	GetGRPCConfig() GRPCServerParameters

	// GetAccessKeyConfig returns the access key configuration
	GetAccessKeyConfig() AccessKeyConfig

	// GetPeers returns a list of other servers participating in this proxy cluster
	GetPeers() []string

	GetPeerManagementType() string

	GetRedisPeerManagement() RedisPeerManagementConfig

	// GetHoneycombAPI returns the base URL (protocol, hostname, and port) of
	// the upstream Honeycomb API server
	GetHoneycombAPI() string

	GetTracesConfig() TracesConfig

	// GetLoggerType returns the type of the logger to use. Valid types are in
	// the logger package
	GetLoggerType() string

	// GetLoggerLevel returns the level of the logger to use.
	GetLoggerLevel() Level

	// GetHoneycombLoggerConfig returns the config specific to the HoneycombLogger
	GetHoneycombLoggerConfig() HoneycombLoggerConfig

	// GetStdoutLoggerConfig returns the config specific to the StdoutLogger
	GetStdoutLoggerConfig() StdoutLoggerConfig

	// GetCollectionConfig returns the config specific to the InMemCollector
	GetCollectionConfig() CollectionConfig

	// GetDisableRedistribution returns whether redistribution is disabled.
	GetDisableRedistribution() bool

	// GetHealthCheckTimeout returns the timeout for Refinery's internal health checks used in the collector
	GetHealthCheckTimeout() time.Duration

	// DetermineSamplerKey returns the key to look up which sampler to use for the given API key, environment, and dataset.
	DetermineSamplerKey(apiKey, env, dataset string) string

	// GetSamplingKeyFieldsForDestName returns the key fields and non-root fields
	// for the given destination (environment, or dataset in classic)
	GetSamplingKeyFieldsForDestName(samplerKey string) []string

	// GetSamplerConfigForDestName returns the sampler type and name to use for
	// the given destination (environment, or dataset in classic)
	GetSamplerConfigForDestName(string) (interface{}, string)

	// GetAllSamplerRules returns all rules in a single map, including the default rules
	GetAllSamplerRules() *V2SamplerConfig

	// GetGeneralConfig returns the config specific to General
	GetGeneralConfig() GeneralConfig

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

	GetIdentifierInterfaceName() string

	GetOTelTracingConfig() OTelTracingConfig

	GetUseIPV6Identifier() bool

	GetRedisIdentifier() string

	// GetDebugServiceAddr sets the IP and port the debug service will run on (you must provide the
	// command line flag -d to start the debug service)
	GetDebugServiceAddr() string

	GetIsDryRun() bool

	GetAddHostMetadataToTrace() bool

	GetAddRuleReasonToTrace() bool

	GetEnvironmentCacheTTL() time.Duration

	GetDatasetPrefix() string

	// GetQueryAuthToken returns the token that must be used to access the /query endpoints
	GetQueryAuthToken() string

	GetPeerTimeout() time.Duration

	GetAdditionalErrorFields() []string

	GetAddSpanCountToRoot() bool

	GetAddCountsToRoot() bool

	GetConfigMetadata() []ConfigMetadata

	GetSampleCacheConfig() SampleCacheConfig

	GetStressReliefConfig() StressReliefConfig

	GetAdditionalAttributes() map[string]string

	GetTraceIdFieldNames() []string

	GetParentIdFieldNames() []string

	GetOpAMPConfig() OpAMPConfig
}

type ConfigReloadCallback func(configHash, ruleCfgHash string)

type ConfigMetadata struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	Hash     string `json:"hash"`
	LoadedAt string `json:"loaded_at"`
}

// ReloadedConfigData holds the new config data that will be applied to
// the Config instance through `Reload` method.
type ReloadedConfigData struct {
	configs []configData
	rules   []configData
}

// ReloadedConfigDataOption is a function that allows setting the new config data
type ReloadedConfigDataOption func(*ReloadedConfigData)

func WithConfigData(in configData) ReloadedConfigDataOption {
	return func(c *ReloadedConfigData) {
		c.configs = append(c.configs, in)
	}
}
func WithRulesData(in configData) ReloadedConfigDataOption {
	return func(c *ReloadedConfigData) {
		c.rules = append(c.rules, in)
	}
}

func IsLegacyAPIKey(key string) bool {
	keyLen := len(key)

	switch keyLen {
	case 32:
		// Check if all characters are hex digits (0-9, a-f)
		for i := 0; i < keyLen; i++ {
			c := key[i]
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
				return false
			}
		}
		return true
	case 64:
		// Check the prefix pattern "hc[a-z]ic_"
		if key[:2] != "hc" || key[3:6] != "ic_" {
			return false
		}
		if key[2] < 'a' || key[2] > 'z' {
			return false
		}

		// Check if the remaining characters are alphanumeric lowercase
		for i := 6; i < keyLen; i++ {
			c := key[i]
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z')) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// GetKeyFields returns the fields that should be used as keys for the sampler.
// It returns two slices: the first contains all fields, including those with the root prefix,
// and the second contains fields that do not have the root prefix.
// Fields that start with "?." are ignored since they should not exist as a field in a trace.
func GetKeyFields(fields []string) (allFields []string, nonRootFields []string) {
	if len(fields) == 0 {
		return nil, nil
	}

	rootFields := make([]string, 0, len(fields))
	nonRootFields = make([]string, 0, len(fields))

	for _, field := range fields {
		switch {
		case field[0] == RootPrefixFirstChar && strings.HasPrefix(field, RootPrefix):
			// If the field starts with "root.", add it to rootFields
			rootFields = append(rootFields, field[len(RootPrefix):])
		case field[0] == ComputedFieldFirstChar && strings.HasPrefix(field, ComputedFieldPrefix):
			// If the field starts with "?.", skip it
		default:
			// Otherwise, add it to nonRootFields
			nonRootFields = append(nonRootFields, field)
		}
	}

	if len(rootFields) == 0 && len(nonRootFields) == 0 {
		return nil, nil
	}

	if len(rootFields) == 0 {
		return nonRootFields, nonRootFields
	}

	return slices.Compact(append(rootFields, nonRootFields...)), nonRootFields
}
