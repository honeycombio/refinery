package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// SerializeToYAML serializes the Config to a YAML string
func SerializeToYAML(cfg Config) (string, string, error) {
	// Create a configContents struct and populate it from the Config interface
	contents := populateConfigContents(cfg)

	yamlBytes, err := yaml.Marshal(contents)
	if err != nil {
		return "", "", fmt.Errorf("error serializing config to YAML: %w", err)
	}

	rules := cfg.GetAllSamplerRules()
	rulesYamlBytes, err := yaml.Marshal(rules)
	if err != nil {
		return "", "", fmt.Errorf("error serializing rules to YAML: %w", err)
	}

	return string(yamlBytes), string(rulesYamlBytes), nil
}

// populateConfigContents creates a configContents struct from a Config interface
func populateConfigContents(cfg Config) configContents {
	opAMPConfig := cfg.GetOpAMPConfig()

	return configContents{
		General: cfg.GetGeneralConfig(),
		Network: NetworkConfig{
			ListenAddr:      cfg.GetListenAddr(),
			PeerListenAddr:  cfg.GetPeerListenAddr(),
			HoneycombAPI:    cfg.GetHoneycombAPI(),
			HTTPIdleTimeout: Duration(cfg.GetHTTPIdleTimeout()),
		},
		OpAMP: OpAMPConfig{
			Endpoint:    opAMPConfig.Endpoint,
			Enabled:     opAMPConfig.Enabled,
			RecordUsage: getDefaultTrueValue(cfg.GetOpAMPConfig().RecordUsage.Get()),
		},
		AccessKeys: cfg.GetAccessKeyConfig(),
		Telemetry:  getRefineryTelemetryConfig(cfg),
		Traces:     cfg.GetTracesConfig(),
		Debugging: DebuggingConfig{
			DebugServiceAddr:      cfg.GetDebugServiceAddr(),
			QueryAuthToken:        cfg.GetQueryAuthToken(),
			AdditionalErrorFields: cfg.GetAdditionalErrorFields(),
			DryRun:                cfg.GetIsDryRun(),
		},
		Logger: LoggerConfig{
			Type:  cfg.GetLoggerType(),
			Level: cfg.GetLoggerLevel(),
		},
		HoneycombLogger:     cfg.GetHoneycombLoggerConfig(),
		StdoutLogger:        cfg.GetStdoutLoggerConfig(),
		PrometheusMetrics:   cfg.GetPrometheusMetricsConfig(),
		LegacyMetrics:       cfg.GetLegacyMetricsConfig(),
		OTelMetrics:         cfg.GetOTelMetricsConfig(),
		OTelTracing:         cfg.GetOTelTracingConfig(),
		PeerManagement:      getPeerManagementConfig(cfg),
		RedisPeerManagement: cfg.GetRedisPeerManagement(),
		Collection:          cfg.GetCollectionConfig(),
		BufferSizes: BufferSizeConfig{
			UpstreamBufferSize: cfg.GetUpstreamBufferSize(),
			PeerBufferSize:     cfg.GetPeerBufferSize(),
		},
		Specialized: SpecializedConfig{
			EnvironmentCacheTTL:       Duration(cfg.GetEnvironmentCacheTTL()),
			CompressPeerCommunication: getDefaultTrueValue(cfg.GetCompressPeerCommunication()),
			AdditionalAttributes:      cfg.GetAdditionalAttributes(),
		},
		IDFieldNames: IDFieldsConfig{
			TraceNames:  cfg.GetTraceIdFieldNames(),
			ParentNames: cfg.GetParentIdFieldNames(),
		},
		GRPCServerParameters: cfg.GetGRPCConfig(),
		SampleCache:          cfg.GetSampleCacheConfig(),
		StressRelief:         cfg.GetStressReliefConfig(),
	}
}

// Helper function to get RefineryTelemetryConfig
func getRefineryTelemetryConfig(cfg Config) RefineryTelemetryConfig {
	return RefineryTelemetryConfig{
		AddRuleReasonToTrace:   cfg.GetAddRuleReasonToTrace(),
		AddSpanCountToRoot:     getDefaultTrueValue(cfg.GetAddSpanCountToRoot()),
		AddCountsToRoot:        cfg.GetAddCountsToRoot(),
		AddHostMetadataToTrace: getDefaultTrueValue(cfg.GetAddHostMetadataToTrace()),
	}
}

// Helper function to get PeerManagementConfig
func getPeerManagementConfig(cfg Config) PeerManagementConfig {
	return PeerManagementConfig{
		Type:                    cfg.GetPeerManagementType(),
		Identifier:              cfg.GetRedisIdentifier(),
		IdentifierInterfaceName: cfg.GetIdentifierInterfaceName(),
		UseIPV6Identifier:       cfg.GetUseIPV6Identifier(),
		Peers:                   cfg.GetPeers(),
	}
}

func getDefaultTrueValue(value bool) *DefaultTrue {
	dt := DefaultTrue(value)
	return &dt
}
