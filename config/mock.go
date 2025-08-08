package config

import (
	"fmt"
	"sync"
	"time"
)

// MockConfig will respond with whatever config it's set to do during
// initialization
type MockConfig struct {
	Callbacks                        []ConfigReloadCallback
	GetAccessKeyConfigVal            AccessKeyConfig
	GetCollectorTypeVal              string
	GetCollectionConfigVal           CollectionConfig
	GetTracesConfigVal               TracesConfig
	GetHoneycombAPIVal               string
	GetListenAddrVal                 string
	GetPeerListenAddrVal             string
	GetHTTPIdleTimeoutVal            time.Duration
	GetCompressPeerCommunicationsVal bool
	GetGRPCEnabledVal                bool
	GetGRPCListenAddrVal             string
	GetGRPCServerParameters          GRPCServerParameters
	GetLoggerTypeVal                 string
	GetHoneycombLoggerConfigVal      HoneycombLoggerConfig
	GetStdoutLoggerConfigVal         StdoutLoggerConfig
	GetLoggerLevelVal                Level
	GetPeersVal                      []string
	GetRedisPeerManagementVal        RedisPeerManagementConfig
	GetSamplerTypeName               string
	GetSamplerTypeVal                interface{}
	GetMetricsTypeVal                string
	GetGeneralConfigVal              GeneralConfig
	GetPrometheusMetricsConfigVal    PrometheusMetricsConfig
	GetOpAmpConfigVal                OpAMPConfig
	GetOTelMetricsConfigVal          OTelMetricsConfig
	GetOTelTracingConfigVal          OTelTracingConfig
	IdentifierInterfaceName          string
	UseIPV6Identifier                bool
	RedisIdentifier                  string
	PeerManagementType               string
	DebugServiceAddr                 string
	DryRun                           bool
	DryRunFieldName                  string
	AddHostMetadataToTrace           bool
	AddRuleReasonToTrace             bool
	EnvironmentCacheTTL              time.Duration
	DatasetPrefix                    string
	QueryAuthToken                   string
	PeerTimeout                      time.Duration
	AdditionalErrorFields            []string
	AddSpanCountToRoot               bool
	AddCountsToRoot                  bool
	CacheOverrunStrategy             string
	SampleCache                      SampleCacheConfig
	StressRelief                     StressReliefConfig
	AdditionalAttributes             map[string]string
	TraceIdFieldNames                []string
	ParentIdFieldNames               []string
	CfgMetadata                      []ConfigMetadata
	CfgHash                          string
	RulesHash                        string

	Mux sync.RWMutex
}

// assert that MockConfig implements Config
var _ Config = (*MockConfig)(nil)

func (m *MockConfig) Reload(opts ...ReloadedConfigDataOption) error {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	for _, callback := range m.Callbacks {
		callback("", "")
	}

	return nil
}

func (m *MockConfig) RegisterReloadCallback(callback ConfigReloadCallback) {
	m.Mux.Lock()
	m.Callbacks = append(m.Callbacks, callback)
	m.Mux.Unlock()
}

func (m *MockConfig) GetHashes() (string, string) {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.CfgHash, m.RulesHash
}

func (m *MockConfig) GetAccessKeyConfig() AccessKeyConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetAccessKeyConfigVal
}

func (m *MockConfig) GetCollectorType() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetCollectorTypeVal
}

func (m *MockConfig) GetCollectionConfig() CollectionConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetCollectionConfigVal
}

func (m *MockConfig) GetHealthCheckTimeout() time.Duration {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return time.Duration(m.GetCollectionConfigVal.HealthCheckTimeout)
}

func (m *MockConfig) GetTracesConfig() TracesConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetTracesConfigVal
}

func (m *MockConfig) GetHoneycombAPI() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetHoneycombAPIVal
}

func (m *MockConfig) GetListenAddr() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetListenAddrVal
}

func (m *MockConfig) GetPeerListenAddr() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetPeerListenAddrVal
}

func (m *MockConfig) GetHTTPIdleTimeout() time.Duration {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetHTTPIdleTimeoutVal
}

func (m *MockConfig) GetCompressPeerCommunication() bool {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetCompressPeerCommunicationsVal
}

func (m *MockConfig) GetGRPCEnabled() bool {
	m.Mux.RLock()
	defer m.Mux.RUnlock()
	return m.GetGRPCEnabledVal
}

func (m *MockConfig) GetGRPCListenAddr() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetGRPCListenAddrVal
}

func (m *MockConfig) GetLoggerType() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetLoggerTypeVal
}

func (m *MockConfig) GetHoneycombLoggerConfig() HoneycombLoggerConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetHoneycombLoggerConfigVal
}

func (m *MockConfig) GetStdoutLoggerConfig() StdoutLoggerConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetStdoutLoggerConfigVal
}

func (m *MockConfig) GetLoggerLevel() Level {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetLoggerLevelVal
}

func (m *MockConfig) GetPeers() []string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetPeersVal
}

func (m *MockConfig) GetRedisPeerManagement() RedisPeerManagementConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetRedisPeerManagementVal
}

func (m *MockConfig) GetGeneralConfig() GeneralConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetGeneralConfigVal
}

func (m *MockConfig) GetPrometheusMetricsConfig() PrometheusMetricsConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetPrometheusMetricsConfigVal
}

func (m *MockConfig) GetOTelMetricsConfig() OTelMetricsConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetOTelMetricsConfigVal
}

func (m *MockConfig) GetOTelTracingConfig() OTelTracingConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetOTelTracingConfigVal
}

// TODO: allow per-dataset mock values
func (m *MockConfig) GetSamplerConfigForDestName(dataset string) (interface{}, string) {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.GetSamplerTypeVal, m.GetSamplerTypeName
}

// GetAllSamplerRules normally returns all dataset rules, including the default
// In this mock, it returns only the rules for "dataset1" according to the type of the value field
func (m *MockConfig) GetAllSamplerRules() *V2SamplerConfig {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	choice := &V2SamplerChoice{}
	switch sampler := m.GetSamplerTypeVal.(type) {
	case *DeterministicSamplerConfig:
		choice.DeterministicSampler = sampler
	case *DynamicSamplerConfig:
		choice.DynamicSampler = sampler
	case *EMADynamicSamplerConfig:
		choice.EMADynamicSampler = sampler
	case *RulesBasedSamplerConfig:
		choice.RulesBasedSampler = sampler
	case *TotalThroughputSamplerConfig:
		choice.TotalThroughputSampler = sampler
	default:
		return nil
	}

	v := &V2SamplerConfig{
		Samplers: map[string]*V2SamplerChoice{"dataset1": choice},
	}

	return v
}

func (m *MockConfig) GetIdentifierInterfaceName() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.IdentifierInterfaceName
}

func (m *MockConfig) GetUseIPV6Identifier() bool {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.UseIPV6Identifier
}

func (m *MockConfig) GetRedisIdentifier() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.RedisIdentifier
}

func (m *MockConfig) GetPeerManagementType() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.PeerManagementType
}

func (m *MockConfig) GetDebugServiceAddr() string {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.DebugServiceAddr
}

func (m *MockConfig) GetIsDryRun() bool {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.DryRun
}

func (m *MockConfig) GetAddHostMetadataToTrace() bool {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.AddHostMetadataToTrace
}

func (m *MockConfig) GetAddRuleReasonToTrace() bool {
	m.Mux.RLock()
	defer m.Mux.RUnlock()

	return m.AddRuleReasonToTrace
}

func (f *MockConfig) GetEnvironmentCacheTTL() time.Duration {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.EnvironmentCacheTTL
}

func (f *MockConfig) GetDatasetPrefix() string {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.DatasetPrefix
}

func (f *MockConfig) GetQueryAuthToken() string {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.QueryAuthToken
}

func (f *MockConfig) GetGRPCConfig() GRPCServerParameters {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.GetGRPCServerParameters
}

func (f *MockConfig) GetPeerTimeout() time.Duration {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.PeerTimeout
}

func (f *MockConfig) GetAdditionalErrorFields() []string {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.AdditionalErrorFields
}

func (f *MockConfig) GetAddSpanCountToRoot() bool {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.AddSpanCountToRoot
}

func (f *MockConfig) GetAddCountsToRoot() bool {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.AddSpanCountToRoot
}

func (f *MockConfig) GetSampleCacheConfig() SampleCacheConfig {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.SampleCache
}

func (f *MockConfig) GetStressReliefConfig() StressReliefConfig {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.StressRelief
}
func (f *MockConfig) GetTraceIdFieldNames() []string {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.TraceIdFieldNames
}

func (f *MockConfig) GetParentIdFieldNames() []string {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.ParentIdFieldNames
}

func (f *MockConfig) GetConfigMetadata() []ConfigMetadata {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.CfgMetadata
}

func (f *MockConfig) GetAdditionalAttributes() map[string]string {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.AdditionalAttributes
}

func (f *MockConfig) GetOpAMPConfig() OpAMPConfig {
	f.Mux.RLock()
	defer f.Mux.RUnlock()

	return f.GetOpAmpConfigVal
}

func (f *MockConfig) SetMaxAlloc(v MemorySize) {
	f.Mux.Lock()
	defer f.Mux.Unlock()

	f.GetCollectionConfigVal.MaxAlloc = v
}

func (f *MockConfig) DetermineSamplerKey(apiKey, env, dataset string) string {
	if IsLegacyAPIKey(apiKey) {
		if f.DatasetPrefix != "" {
			return fmt.Sprintf("%s.%s", f.DatasetPrefix, dataset)
		}
		return dataset
	}

	return env
}

func (f *MockConfig) GetSamplingKeyFieldsForDestName(samplerKey string) []string {
	switch sampler := f.GetSamplerTypeVal.(type) {
	case *DeterministicSamplerConfig:
		return sampler.GetSamplingFields()
	case *DynamicSamplerConfig:
		return sampler.GetSamplingFields()
	case *EMADynamicSamplerConfig:
		return sampler.GetSamplingFields()
	case *RulesBasedSamplerConfig:
		return sampler.GetSamplingFields()
	case *TotalThroughputSamplerConfig:
		return sampler.GetSamplingFields()
	default:
		return nil
	}

}
