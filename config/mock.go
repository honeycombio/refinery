package config

import (
	"encoding/json"
	"time"
)

// MockConfig will respond with whatever config it's set to do during
// initialization
type MockConfig struct {
	Callbacks                     []func()
	GetAPIKeysErr                 error
	GetAPIKeysVal                 []string
	GetCollectorTypeErr           error
	GetCollectorTypeVal           string
	GetInMemoryCollectorConfigErr error
	// GetInMemoryCollectorConfigVal must be a JSON representation of the config struct to be populated.
	GetInMemoryCollectorConfigVal string
	GetHoneycombAPIErr            error
	GetHoneycombAPIVal            string
	GetListenAddrErr              error
	GetListenAddrVal              string
	GetPeerListenAddrErr          error
	GetPeerListenAddrVal          string
	GetLoggerTypeErr              error
	GetLoggerTypeVal              string
	GetHoneycombLoggerConfigErr   error
	// GetHoneycombLoggerConfigVal must be a JSON representation of the config struct to be populated.
	GetHoneycombLoggerConfigVal string
	GetLoggingLevelErr          error
	GetLoggingLevelVal          string
	GetOtherConfigErr           error
	// GetOtherConfigVal must be a JSON representation of the config struct to be populated.
	GetOtherConfigVal            string
	GetPeersErr                  error
	GetPeersVal                  []string
	GetRedisHostErr              error
	GetRedisHostVal              string
	GetSamplerTypeErr            error
	GetSamplerTypeVal            string
	GetMetricsTypeErr            error
	GetMetricsTypeVal            string
	GetHoneycombMetricsConfigErr error
	// GetHoneycombMetricsConfigVal must be a JSON representation of the config struct to be populated.
	GetHoneycombMetricsConfigVal  string
	GetPrometheusMetricsConfigErr error
	// GetPrometheusMetricsConfigVal must be a JSON representation of the config struct to be populated.
	GetPrometheusMetricsConfigVal string
	GetSendDelayErr               error
	GetSendDelayVal               time.Duration
	GetTraceTimeoutErr            error
	GetTraceTimeoutVal            time.Duration
	GetUpstreamBufferSizeVal      int
	GetPeerBufferSizeVal          int
	SendTickerVal                 time.Duration
	IdentifierInterfaceName       string
	UseIPV6Identifier             bool
	RedisIdentifier               string
	PeerManagementType            string
	DebugServiceAddr              string
	DryRun                        bool
}

func (m *MockConfig) ReloadConfig() {
	for _, callback := range m.Callbacks {
		callback()
	}
}
func (m *MockConfig) RegisterReloadCallback(callback func()) {
	m.Callbacks = append(m.Callbacks, callback)
}
func (m *MockConfig) GetAPIKeys() ([]string, error) { return m.GetAPIKeysVal, m.GetAPIKeysErr }
func (m *MockConfig) GetCollectorType() (string, error) {
	return m.GetCollectorTypeVal, m.GetCollectorTypeErr
}
func (m *MockConfig) GetInMemCollectorConfig(imcConfig *InMemoryCollectorConfig) error {
	err := json.Unmarshal([]byte(m.GetInMemoryCollectorConfigVal), imcConfig)
	if err != nil {
		return err
	}
	return m.GetInMemoryCollectorConfigErr
}
func (m *MockConfig) GetHoneycombAPI() (string, error) {
	return m.GetHoneycombAPIVal, m.GetHoneycombAPIErr
}
func (m *MockConfig) GetListenAddr() (string, error) { return m.GetListenAddrVal, m.GetListenAddrErr }
func (m *MockConfig) GetPeerListenAddr() (string, error) {
	return m.GetPeerListenAddrVal, m.GetPeerListenAddrErr
}
func (m *MockConfig) GetLoggerType() (string, error) { return m.GetLoggerTypeVal, m.GetLoggerTypeErr }
func (m *MockConfig) GetHoneycombLoggerConfig(hlConfig *HoneycombLoggerConfig) error {
	err := json.Unmarshal([]byte(m.GetHoneycombLoggerConfigVal), hlConfig)
	if err != nil {
		return err
	}
	return m.GetHoneycombLoggerConfigErr
}
func (m *MockConfig) GetLoggingLevel() (string, error) {
	return m.GetLoggingLevelVal, m.GetLoggingLevelErr
}
func (m *MockConfig) GetOtherConfig(name string, iface interface{}) error {
	err := json.Unmarshal([]byte(m.GetOtherConfigVal), iface)
	if err != nil {
		return err
	}
	return m.GetOtherConfigErr
}
func (m *MockConfig) GetPeers() ([]string, error)   { return m.GetPeersVal, m.GetPeersErr }
func (m *MockConfig) GetRedisHost() (string, error) { return m.GetRedisHostVal, m.GetRedisHostErr }
func (m *MockConfig) GetMetricsType() (string, error) {
	return m.GetMetricsTypeVal, m.GetMetricsTypeErr
}
func (m *MockConfig) GetHoneycombMetricsConfig(hmConfig *HoneycombMetricsConfig) error {
	err := json.Unmarshal([]byte(m.GetHoneycombMetricsConfigVal), hmConfig)
	if err != nil {
		return err
	}
	return m.GetHoneycombMetricsConfigErr
}
func (m *MockConfig) GetPrometheusMetricsConfig(pmConfig *PrometheusMetricsConfig) error {
	err := json.Unmarshal([]byte(m.GetPrometheusMetricsConfigVal), pmConfig)
	if err != nil {
		return err
	}
	return m.GetPrometheusMetricsConfigErr
}
func (m *MockConfig) GetSendDelay() (time.Duration, error) {
	return m.GetSendDelayVal, m.GetSendDelayErr
}
func (m *MockConfig) GetTraceTimeout() (time.Duration, error) {
	return m.GetTraceTimeoutVal, m.GetTraceTimeoutErr
}

// TODO: allow per-dataset mock values
func (m *MockConfig) GetSamplerTypeForDataset(dataset string) (string, error) {
	return m.GetSamplerTypeVal, m.GetSamplerTypeErr
}

func (m *MockConfig) GetUpstreamBufferSize() int {
	return m.GetUpstreamBufferSizeVal
}
func (m *MockConfig) GetPeerBufferSize() int {
	return m.GetPeerBufferSizeVal
}

func (m *MockConfig) GetIdentifierInterfaceName() (string, error) {
	return m.IdentifierInterfaceName, nil
}

func (m *MockConfig) GetUseIPV6Identifier() (bool, error) {
	return m.UseIPV6Identifier, nil
}

func (m *MockConfig) GetRedisIdentifier() (string, error) {
	return m.RedisIdentifier, nil
}

func (m *MockConfig) GetSendTickerValue() time.Duration {
	return m.SendTickerVal
}

func (m *MockConfig) GetPeerManagementType() (string, error) {
	return m.PeerManagementType, nil
}

func (m *MockConfig) GetDebugServiceAddr() (string, error) {
	return m.DebugServiceAddr, nil
}

func (m *MockConfig) GetIsDryRun() bool {
	return m.DryRun
}
