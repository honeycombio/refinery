package config

import "encoding/json"

// MockConfig will respond with whatever config it's set to do during
// initialization
type MockConfig struct {
	GetAPIKeysErr        error
	GetAPIKeysVal        []string
	GetCollectorTypeErr  error
	GetCollectorTypeVal  string
	GetHoneycombAPIErr   error
	GetHoneycombAPIVal   string
	GetListenAddrErr     error
	GetListenAddrVal     string
	GetPeerListenAddrErr error
	GetPeerListenAddrVal string
	GetLoggerTypeErr     error
	GetLoggerTypeVal     string
	GetLoggingLevelErr   error
	GetLoggingLevelVal   string
	GetOtherConfigErr    error
	// GetOtherConfigVal must be a JSON representation of the config struct to be populated.
	GetOtherConfigVal        string
	GetPeersErr              error
	GetPeersVal              []string
	GetRedisHostErr          error
	GetRedisHostVal          string
	GetDefaultSamplerTypeErr error
	GetDefaultSamplerTypeVal string
	GetMetricsTypeErr        error
	GetMetricsTypeVal        string
	GetSendDelayErr          error
	GetSendDelayVal          int
	GetSpanSeenDelayErr      error
	GetSpanSeenDelayVal      int
	GetTraceTimeoutErr       error
	GetTraceTimeoutVal       int
	GetUpstreamBufferSizeVal int
	GetPeerBufferSizeVal     int
}

func (m *MockConfig) ReloadConfig()                 {}
func (m *MockConfig) RegisterReloadCallback(func()) {}
func (m *MockConfig) GetAPIKeys() ([]string, error) { return m.GetAPIKeysVal, m.GetAPIKeysErr }
func (m *MockConfig) GetCollectorType() (string, error) {
	return m.GetCollectorTypeVal, m.GetCollectorTypeErr
}
func (m *MockConfig) GetHoneycombAPI() (string, error) {
	return m.GetHoneycombAPIVal, m.GetHoneycombAPIErr
}
func (m *MockConfig) GetListenAddr() (string, error) { return m.GetListenAddrVal, m.GetListenAddrErr }
func (m *MockConfig) GetPeerListenAddr() (string, error) {
	return m.GetPeerListenAddrVal, m.GetPeerListenAddrErr
}
func (m *MockConfig) GetLoggerType() (string, error) { return m.GetLoggerTypeVal, m.GetLoggerTypeErr }
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
func (m *MockConfig) GetDefaultSamplerType() (string, error) {
	return m.GetDefaultSamplerTypeVal, m.GetDefaultSamplerTypeErr
}
func (m *MockConfig) GetMetricsType() (string, error) { return m.GetMetricsTypeVal, m.GetMetricsTypeErr }
func (m *MockConfig) GetSendDelay() (int, error)      { return m.GetSendDelayVal, m.GetSendDelayErr }
func (m *MockConfig) GetSpanSeenDelay() (int, error) {
	return m.GetSpanSeenDelayVal, m.GetSpanSeenDelayErr
}
func (m *MockConfig) GetTraceTimeout() (int, error) { return m.GetTraceTimeoutVal, m.GetTraceTimeoutErr }

// TODO: allow per-dataset mock values
func (m *MockConfig) GetSamplerTypeForDataset(dataset string) (string, error) {
	return m.GetDefaultSamplerTypeVal, m.GetDefaultSamplerTypeErr
}

func (m *MockConfig) GetUpstreamBufferSize() int {
	return m.GetUpstreamBufferSizeVal
}
func (m *MockConfig) GetPeerBufferSize() int {
	return m.GetPeerBufferSizeVal
}
