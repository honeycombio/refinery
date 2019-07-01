package config

import (
	"fmt"

	libhoney "github.com/honeycombio/libhoney-go"
	toml "github.com/pelletier/go-toml"
)

type FileConfig struct {
	Path      string
	conf      confContents
	rawConf   *toml.Tree
	callbacks []func()
}

type confContents struct {
	ListenAddr           string
	PeerListenAddr       string
	PeerAdvertisePort    string
	APIKeys              []string
	Peers                []string
	RedisHost            string
	HoneycombAPI         string
	CollectCacheCapacity int
	Logger               string
	LoggingLevel         string
	Collector            string
	Sampler              string
	Metrics              string
	SendDelay            int
	SpanSeenDelay        int
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
		go callback()
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

func (f *FileConfig) GetPeerListenAddr() (string, error) {
	return f.conf.PeerListenAddr, nil
}

func (f *FileConfig) GetPeerAdvertisePort() (string, error) {
	return f.conf.PeerAdvertisePort, nil
}

func (f *FileConfig) GetAPIKeys() ([]string, error) {
	return f.conf.APIKeys, nil
}

func (f *FileConfig) GetPeers() ([]string, error) {
	return f.conf.Peers, nil
}

func (f *FileConfig) GetRedisHost() (string, error) {
	return f.conf.RedisHost, nil
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

func (f *FileConfig) GetSpanSeenDelay() (int, error) {
	return f.conf.SpanSeenDelay, nil
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
