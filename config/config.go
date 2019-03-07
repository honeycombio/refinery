package config

import (
	"encoding/json"
	"fmt"

	"github.com/honeycombio/libhoney-go"

	toml "github.com/pelletier/go-toml"
)

type Config interface {

	// ReloadConfigs requests the configuration implementation to re-read
	// configs from whatever backs the source. Triggered by sending a USR1
	// signal to Samproxy
	ReloadConfig()

	// RegisterReloadCallback takes a name and a function that will be called
	// when the configuration is reloaded. This will happen infrequently. If
	// consumers of configuration set config values on startup, they should
	// check their values haven't changed and re-start anything that needs
	// restarting with the new values.
	RegisterReloadCallback(callback func())

	// GetListenAddr returns the address and port on which to listen for
	// incoming events
	GetListenAddr() (string, error)

	// GetAPIKeys returns a list of Honeycomb API keys
	GetAPIKeys() ([]string, error)

	// GetPeers returns a list of other servers participating in this proxy cluster
	GetPeers() ([]string, error)

	// GetHoneycombAPI returns the base URL (protocol, hostname, and port) of
	// the upstream Honeycomb API server
	GetHoneycombAPI() (string, error)

	// GetLoggingLevel returns the verbosity with which we should log
	GetLoggingLevel() (string, error)

	// GetSendDelay returns the number of seconds to pause after a trace is
	// complete before sending it, to allow stragglers to arrive
	GetSendDelay() (int, error)

	// GetTraceTimeout is how long to wait before sending a trace even if it's
	// not complete. This should be longer than the longest expected trace
	// duration.
	GetTraceTimeout() (int, error)

	// GetOtherConfig attempts to fill the passed in struct with the contents of
	// a subsection of the config.   This is used by optional configurations to
	// allow different implementations of necessary interfaces configure
	// themselves
	GetOtherConfig(name string, configStruct interface{}) error

	// GetLoggerType returns the type of the logger to use. Valid types are in
	// the logger package
	GetLoggerType() (string, error)

	// GetCollectorType returns the type of the collector to use. Valid types
	// are in the collect package
	GetCollectorType() (string, error)

	// GetDefaultSamplerType returns the sampler type to use for all datasets
	// not explicitly defined
	GetDefaultSamplerType() (string, error)

	// GetSamplerTypeForDataset returns the sampler type to use for the given dataset
	GetSamplerTypeForDataset(string) (string, error)

	// GetMetricsType returns the type of metrics to use. Valid types are in the
	// metrics package
	GetMetricsType() (string, error)

	// GetUpstreamBufferSize returns the size of the libhoney buffer to use for the upstream
	// libhoney client
	GetUpstreamBufferSize() int
	// GetPeerBufferSize returns the size of the libhoney buffer to use for the peer forwarding
	// libhoney client
	GetPeerBufferSize() int
}

type FileConfig struct {
	Path      string
	conf      confContents
	rawConf   *toml.Tree
	callbacks []func()
}

type confContents struct {
	ListenAddr           string
	APIKeys              []string
	Peers                []string
	HoneycombAPI         string
	CollectCacheCapacity int
	Logger               string
	LoggingLevel         string
	Collector            string
	Sampler              string
	Metrics              string
	SendDelay            int
	TraceTimeout         int
	UpstreamBufferSize   int
	PeerBufferSize       int
}

// Used to marshall in the sampler type in SamplerConfig definitions
// other fields are ignored
type samplerConfigType struct {
	Sampler string
}

// Start reads the config initially
func (f *FileConfig) Start() error {
	return f.reloadConfig()
}

// reloadConfig re-reads the config file for up-to-date config options. It is
// called when a USR1 signal hits the process.
func (f *FileConfig) reloadConfig() error {
	config, err := toml.LoadFile(f.Path)
	if err != nil {
		return err
	}
	f.rawConf = config
	f.conf = confContents{}
	err = config.Unmarshal(&f.conf)
	if err != nil {
		return err
	}
	// notify everybody that we've reloaded the config
	for _, callback := range f.callbacks {
		callback()
	}
	return nil
}

func (f *FileConfig) ReloadConfig() {
	err := f.reloadConfig()
	if err != nil {
		fmt.Printf("Error reloading configs: %+v\n", err)
	}
}

func (f *FileConfig) RegisterReloadCallback(cb func()) {
	if f.callbacks == nil {
		f.callbacks = make([]func(), 0, 1)
	}
	f.callbacks = append(f.callbacks, cb)
}

func (f *FileConfig) GetListenAddr() (string, error) {
	return f.conf.ListenAddr, nil
}

func (f *FileConfig) GetAPIKeys() ([]string, error) {
	return f.conf.APIKeys, nil
}

func (f *FileConfig) GetPeers() ([]string, error) {
	return f.conf.Peers, nil
}

func (f *FileConfig) GetHoneycombAPI() (string, error) {
	return f.conf.HoneycombAPI, nil
}

func (f *FileConfig) GetLoggingLevel() (string, error) {
	return f.conf.LoggingLevel, nil
}

func (f *FileConfig) GetLoggerType() (string, error) {
	return f.conf.Logger, nil
}

func (f *FileConfig) GetCollectorType() (string, error) {
	return f.conf.Collector, nil
}

func (f *FileConfig) GetDefaultSamplerType() (string, error) {
	t := samplerConfigType{}
	err := f.GetOtherConfig("SamplerConfig._default", &t)
	if err != nil {
		return "", err
	}
	return t.Sampler, nil
}

func (f *FileConfig) GetSamplerTypeForDataset(dataset string) (string, error) {
	t := samplerConfigType{}
	err := f.GetOtherConfig("SamplerConfig."+dataset, &t)
	if err != nil {
		return "", err
	}
	return t.Sampler, nil
}

func (f *FileConfig) GetMetricsType() (string, error) {
	return f.conf.Metrics, nil
}

func (f *FileConfig) GetSendDelay() (int, error) {
	return f.conf.SendDelay, nil
}

func (f *FileConfig) GetTraceTimeout() (int, error) {
	return f.conf.TraceTimeout, nil
}

func (f *FileConfig) GetOtherConfig(name string, iface interface{}) error {
	subConf := f.rawConf.Get(name)
	if subConfTree, ok := subConf.(*toml.Tree); ok {
		return subConfTree.Unmarshal(iface)
	}
	return fmt.Errorf("failed to find config tree for %s", name)
}

func (f *FileConfig) GetUpstreamBufferSize() int {
	if f.conf.UpstreamBufferSize == 0 {
		return libhoney.DefaultPendingWorkCapacity
	}
	return f.conf.UpstreamBufferSize
}

func (f *FileConfig) GetPeerBufferSize() int {
	if f.conf.PeerBufferSize == 0 {
		return libhoney.DefaultPendingWorkCapacity
	}
	return f.conf.PeerBufferSize
}

// MockConfig will respond with whatever config it's set to do during
// initialization
type MockConfig struct {
	GetAPIKeysErr       error
	GetAPIKeysVal       []string
	GetCollectorTypeErr error
	GetCollectorTypeVal string
	GetHoneycombAPIErr  error
	GetHoneycombAPIVal  string
	GetListenAddrErr    error
	GetListenAddrVal    string
	GetLoggerTypeErr    error
	GetLoggerTypeVal    string
	GetLoggingLevelErr  error
	GetLoggingLevelVal  string
	GetOtherConfigErr   error
	// GetOtherConfigVal must be a JSON representation of the config struct to be populated.
	GetOtherConfigVal        string
	GetPeersErr              error
	GetPeersVal              []string
	GetDefaultSamplerTypeErr error
	GetDefaultSamplerTypeVal string
	GetMetricsTypeErr        error
	GetMetricsTypeVal        string
	GetSendDelayErr          error
	GetSendDelayVal          int
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
func (m *MockConfig) GetPeers() ([]string, error) { return m.GetPeersVal, m.GetPeersErr }
func (m *MockConfig) GetDefaultSamplerType() (string, error) {
	return m.GetDefaultSamplerTypeVal, m.GetDefaultSamplerTypeErr
}
func (m *MockConfig) GetMetricsType() (string, error) { return m.GetMetricsTypeVal, m.GetMetricsTypeErr }
func (m *MockConfig) GetSendDelay() (int, error)      { return m.GetSendDelayVal, m.GetSendDelayErr }
func (m *MockConfig) GetTraceTimeout() (int, error)   { return m.GetTraceTimeoutVal, m.GetTraceTimeoutErr }

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
