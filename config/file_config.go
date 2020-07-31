package config

import (
	"fmt"
	"os"
	"time"

	libhoney "github.com/honeycombio/libhoney-go"
	viper "github.com/spf13/viper"
)

type FileConfig struct {
	ConfigFile string
	RulesFile  string
	conf       confContents
	callbacks  []func()
}

type confContents struct {
	ListenAddr              string
	PeerListenAddr          string
	APIKeys                 []string
	Peers                   []string
	RedisHost               string
	RedisIdentifier         string
	IdentifierInterfaceName string
	UseIPV6Identifier       bool
	HoneycombAPI            string
	CollectCacheCapacity    int
	Logger                  string
	LoggingLevel            string
	Collector               string
	Sampler                 string
	Metrics                 string
	SendDelay               time.Duration
	TraceTimeout            time.Duration
	SendTicker              time.Duration
	UpstreamBufferSize      int
	PeerBufferSize          int
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

// reloadConfig re-reads the config files for up-to-date config options. It is
// called when a USR1 signal hits the process.
func (f *FileConfig) reloadConfig() error {
	viper.SetConfigFile(f.ConfigFile)
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	viper.SetConfigFile(f.RulesFile)
	err = viper.MergeInConfig()
	if err != nil {
		return err
	}

	// set the defaults here which are used when the key is not present in the file
	f.conf = confContents{
		SendTicker: 100 * time.Millisecond,
	}

	err = viper.Unmarshal(&f.conf)
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

func (f *FileConfig) GetAPIKeys() ([]string, error) {
	return f.conf.APIKeys, nil
}

func (f *FileConfig) GetPeers() ([]string, error) {
	return f.conf.Peers, nil
}

func (f *FileConfig) GetRedisHost() (string, error) {
	envRedisHost := os.Getenv(RedisHostEnvVarName)
	if envRedisHost != "" {
		return envRedisHost, nil
	}
	return f.conf.RedisHost, nil
}

func (f *FileConfig) GetIdentifierInterfaceName() (string, error) {
	return f.conf.IdentifierInterfaceName, nil
}

func (f *FileConfig) GetUseIPV6Identifier() (bool, error) {
	return f.conf.UseIPV6Identifier, nil
}

func (f *FileConfig) GetRedisIdentifier() (string, error) {
	return f.conf.RedisIdentifier, nil
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

func (f *FileConfig) GetSendDelay() (time.Duration, error) {
	return f.conf.SendDelay, nil
}

func (f *FileConfig) GetTraceTimeout() (time.Duration, error) {
	return f.conf.TraceTimeout, nil
}

func (f *FileConfig) GetOtherConfig(name string, iface interface{}) error {
	subConf := viper.Sub(name)
	if subConf != nil {
		return subConf.Unmarshal(iface)
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

func (f *FileConfig) GetSendTickerValue() time.Duration {
	return f.conf.SendTicker
}
