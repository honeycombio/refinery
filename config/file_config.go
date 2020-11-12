package config

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-playground/validator"
	libhoney "github.com/honeycombio/libhoney-go"
	viper "github.com/spf13/viper"
)

type fileConfig struct {
	config        *viper.Viper
	rules         *viper.Viper
	conf          *configContents
	callbacks     []func()
	errorCallback func(error)
	mux           sync.RWMutex
}

type RulesBasedSamplerCondition struct {
	Field    string
	Operator string
	Value    interface{}
}

func (r *RulesBasedSamplerCondition) String() string {
	return fmt.Sprintf("%+v", *r)
}

type RulesBasedSamplerRule struct {
	Name       string
	SampleRate int
	Drop       bool
	Condition  []*RulesBasedSamplerCondition
}

func (r *RulesBasedSamplerRule) String() string {
	return fmt.Sprintf("%+v", *r)
}

type RulesBasedSamplerConfig struct {
	Rule []*RulesBasedSamplerRule
}

func (r *RulesBasedSamplerConfig) String() string {
	return fmt.Sprintf("%+v", *r)
}

type configContents struct {
	ListenAddr         string        `validate:"required"`
	PeerListenAddr     string        `validate:"required"`
	APIKeys            []string      `validate:"required"`
	HoneycombAPI       string        `validate:"required,url"`
	Logger             string        `validate:"required,oneof= logrus honeycomb"`
	LoggingLevel       string        `validate:"required"`
	Collector          string        `validate:"required,oneof= InMemCollector"`
	Sampler            string        `validate:"required,oneof= DeterministicSampler DynamicSampler EMADynamicSampler RulesBasedSampler TotalThroughputSampler"`
	Metrics            string        `validate:"required,oneof= prometheus honeycomb"`
	SendDelay          time.Duration `validate:"required"`
	TraceTimeout       time.Duration `validate:"required"`
	SendTicker         time.Duration `validate:"required"`
	UpstreamBufferSize int           `validate:"required"`
	PeerBufferSize     int           `validate:"required"`
	DebugServiceAddr   string
	DryRun             bool
	DryRunFieldName    string
	PeerManagement     PeerManagementConfig           `validate:"required"`
	InMemCollector     InMemoryCollectorCacheCapacity `validate:"required"`
}

type InMemoryCollectorCacheCapacity struct {
	// CacheCapacity must be less than math.MaxInt32
	CacheCapacity int `validate:"required,lt=2147483647"`
	MaxAlloc      uint64
}

type HoneycombLevel int

type HoneycombLoggerConfig struct {
	LoggerHoneycombAPI string `validate:"required,url"`
	LoggerAPIKey       string `validate:"required"`
	LoggerDataset      string `validate:"required"`
	Level              HoneycombLevel
}

type PrometheusMetricsConfig struct {
	MetricsListenAddr string `validate:"required"`
}

type HoneycombMetricsConfig struct {
	MetricsHoneycombAPI      string `validate:"required,url"`
	MetricsAPIKey            string `validate:"required"`
	MetricsDataset           string `validate:"required"`
	MetricsReportingInterval int64  `validate:"required"`
}

type PeerManagementConfig struct {
	Type                    string   `validate:"required,oneof= file redis"`
	Peers                   []string `validate:"dive,url"`
	RedisHost               string
	IdentifierInterfaceName string
	UseIPV6Identifier       bool
	RedisIdentifier         string
}

// NewConfig creates a new config struct
func NewConfig(config, rules string, errorCallback func(error)) (Config, error) {
	c := viper.New()

	c.BindEnv("PeerManagement.RedisHost", "REFINERY_REDIS_HOST")
	c.SetDefault("ListenAddr", "0.0.0.0:8080")
	c.SetDefault("PeerListenAddr", "0.0.0.0:8081")
	c.SetDefault("APIKeys", []string{"*"})
	c.SetDefault("PeerManagement.Peers", []string{"http://127.0.0.1:8081"})
	c.SetDefault("PeerManagement.Type", "file")
	c.SetDefault("PeerManagement.UseIPV6Identifier", false)
	c.SetDefault("HoneycombAPI", "https://api.honeycomb.io")
	c.SetDefault("Logger", "logrus")
	c.SetDefault("LoggingLevel", "debug")
	c.SetDefault("Collector", "InMemCollector")
	c.SetDefault("Sampler", "DynamicSampler")
	c.SetDefault("Metrics", "honeycomb")
	c.SetDefault("SendDelay", 2*time.Second)
	c.SetDefault("TraceTimeout", 60*time.Second)
	c.SetDefault("SendTicker", 100*time.Millisecond)
	c.SetDefault("UpstreamBufferSize", libhoney.DefaultPendingWorkCapacity)
	c.SetDefault("PeerBufferSize", libhoney.DefaultPendingWorkCapacity)
	c.SetDefault("DryRun", false)
	c.SetDefault("DryRunFieldName", "refinery_kept")
	c.SetDefault("MaxAlloc", uint64(0))

	c.SetConfigFile(config)
	err := c.ReadInConfig()

	if err != nil {
		return nil, err
	}

	r := viper.New()

	r.SetDefault("Sampler", "DeterministicSampler")
	r.SetDefault("SampleRate", 1)

	r.SetConfigFile(rules)
	err = r.ReadInConfig()

	if err != nil {
		return nil, err
	}

	fc := &fileConfig{
		config:        c,
		rules:         r,
		conf:          &configContents{},
		callbacks:     make([]func(), 0),
		errorCallback: errorCallback,
	}

	err = fc.unmarshal()

	if err != nil {
		return nil, err
	}

	v := validator.New()
	err = v.Struct(fc.conf)
	if err != nil {
		return nil, err
	}

	err = fc.validateConditionalConfigs()
	if err != nil {
		return nil, err
	}

	err = fc.validateSamplerConfigs()
	if err != nil {
		return nil, err
	}

	c.WatchConfig()
	c.OnConfigChange(fc.onChange)

	r.WatchConfig()
	r.OnConfigChange(fc.onChange)

	return fc, nil
}

func (f *fileConfig) onChange(in fsnotify.Event) {
	v := validator.New()
	err := v.Struct(f.conf)
	if err != nil {
		f.errorCallback(err)
		return
	}

	err = f.validateConditionalConfigs()
	if err != nil {
		f.errorCallback(err)
		return
	}

	err = f.validateSamplerConfigs()
	if err != nil {
		f.errorCallback(err)
		return
	}

	f.unmarshal()

	f.mux.RLock()
	defer f.mux.RUnlock()

	for _, c := range f.callbacks {
		c()
	}
}

func (f *fileConfig) unmarshal() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	err := f.config.Unmarshal(f.conf)

	if err != nil {
		return err
	}

	err = f.rules.Unmarshal(f.conf)

	if err != nil {
		return err
	}

	return nil
}

func (f *fileConfig) validateConditionalConfigs() error {
	// validate logger config
	loggerType, err := f.GetLoggerType()
	if err != nil {
		return err
	}
	if loggerType == "honeycomb" {
		_, err = f.GetHoneycombLoggerConfig()
		if err != nil {
			return err
		}
	}

	// validate metrics config
	metricsType, err := f.GetMetricsType()
	if err != nil {
		return err
	}
	if metricsType == "honeycomb" {
		_, err = f.GetHoneycombMetricsConfig()
		if err != nil {
			return err
		}
	}
	if metricsType == "prometheus" {
		_, err = f.GetPrometheusMetricsConfig()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *fileConfig) validateSamplerConfigs() error {
	keys := f.rules.AllKeys()
	for _, key := range keys {
		parts := strings.Split(key, ".")

		// verify default sampler config
		if parts[0] == "sampler" {
			t := f.rules.GetString(key)
			var i interface{}
			switch t {
			case "DeterministicSampler":
				i = &DeterministicSamplerConfig{}
			case "DynamicSampler":
				i = &DynamicSamplerConfig{}
			case "EMADynamicSampler":
				i = &EMADynamicSamplerConfig{}
			case "RulesBasedSampler":
				i = &RulesBasedSamplerConfig{}
			default:
				return errors.New("Invalid or missing default sampler type")
			}
			err := f.rules.Unmarshal(i)
			if err != nil {
				return err
			}
			v := validator.New()
			err = v.Struct(i)
			if err != nil {
				return err
			}
		}

		// verify dataset sampler configs
		if len(parts) > 1 && parts[1] == "sampler" {
			t := f.rules.GetString(key)
			var i interface{}
			switch t {
			case "DeterministicSampler":
				i = &DeterministicSamplerConfig{}
			case "DynamicSampler":
				i = &DynamicSamplerConfig{}
			case "EMADynamicSampler":
				i = &EMADynamicSamplerConfig{}
			case "RulesBasedSampler":
				i = &RulesBasedSamplerConfig{}
			default:
				return errors.New("Invalid or missing dataset sampler type")
			}
			datasetName := parts[0]
			if sub := f.rules.Sub(datasetName); sub != nil {
				err := sub.Unmarshal(i)
				if err != nil {
					return err
				}
				v := validator.New()
				err = v.Struct(i)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (f *fileConfig) RegisterReloadCallback(cb func()) {
	f.mux.Lock()
	defer f.mux.Unlock()

	f.callbacks = append(f.callbacks, cb)
}

func (f *fileConfig) GetListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.conf.ListenAddr)
	if err != nil {
		return "", err
	}
	return f.conf.ListenAddr, nil
}

func (f *fileConfig) GetPeerListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.conf.PeerListenAddr)
	if err != nil {
		return "", err
	}
	return f.conf.PeerListenAddr, nil
}

func (f *fileConfig) GetAPIKeys() ([]string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.APIKeys, nil
}

func (f *fileConfig) GetPeerManagementType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.PeerManagement.Type, nil
}

func (f *fileConfig) GetPeers() ([]string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.PeerManagement.Peers, nil
}

func (f *fileConfig) GetRedisHost() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.config.GetString("PeerManagement.RedisHost"), nil
}

func (f *fileConfig) GetIdentifierInterfaceName() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.config.GetString("PeerManagement.IdentifierInterfaceName"), nil
}

func (f *fileConfig) GetUseIPV6Identifier() (bool, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.config.GetBool("PeerManagement.UseIPV6Identifier"), nil
}

func (f *fileConfig) GetRedisIdentifier() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.config.GetString("PeerManagement.RedisIdentifier"), nil
}

func (f *fileConfig) GetHoneycombAPI() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.HoneycombAPI, nil
}

func (f *fileConfig) GetLoggingLevel() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.LoggingLevel, nil
}

func (f *fileConfig) GetLoggerType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.Logger, nil
}

func (f *fileConfig) GetHoneycombLoggerConfig() (HoneycombLoggerConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	hlConfig := &HoneycombLoggerConfig{}
	if sub := f.config.Sub("HoneycombLogger"); sub != nil {
		err := sub.UnmarshalExact(hlConfig)
		if err != nil {
			return *hlConfig, err
		}

		v := validator.New()
		err = v.Struct(hlConfig)
		if err != nil {
			return *hlConfig, err
		}

		return *hlConfig, nil
	}
	return *hlConfig, errors.New("No config found for HoneycombLogger")
}

func (f *fileConfig) GetCollectorType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.Collector, nil
}

func (f *fileConfig) GetSamplerConfigForDataset(dataset string) (interface{}, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	key := fmt.Sprintf("%s.Sampler", dataset)
	if ok := f.rules.IsSet(key); ok {
		t := f.rules.GetString(key)
		var i interface{}

		switch t {
		case "DeterministicSampler":
			i = &DeterministicSamplerConfig{}
		case "DynamicSampler":
			i = &DynamicSamplerConfig{}
		case "EMADynamicSampler":
			i = &EMADynamicSamplerConfig{}
		case "RulesBasedSampler":
			i = &RulesBasedSamplerConfig{}
		case "TotalThroughputSampler":
			i = &TotalThroughputSamplerConfig{}
		default:
			return nil, errors.New("No Sampler found")
		}

		if sub := f.rules.Sub(dataset); sub != nil {
			return i, sub.Unmarshal(i)
		}

	} else if ok := f.rules.IsSet("Sampler"); ok {
		t := f.rules.GetString("Sampler")
		var i interface{}

		switch t {
		case "DeterministicSampler":
			i = &DeterministicSamplerConfig{}
		case "DynamicSampler":
			i = &DynamicSamplerConfig{}
		case "EMADynamicSampler":
			i = &EMADynamicSamplerConfig{}
		case "RulesBasedSampler":
			i = &RulesBasedSamplerConfig{}
		case "TotalThroughputSampler":
			i = &TotalThroughputSamplerConfig{}
		default:
			return nil, errors.New("No Sampler found")
		}

		return i, f.rules.Unmarshal(i)
	}

	return nil, errors.New("No Sampler found")
}

func (f *fileConfig) GetInMemCollectorCacheCapacity() (InMemoryCollectorCacheCapacity, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	capacity := &InMemoryCollectorCacheCapacity{}
	if sub := f.config.Sub("InMemCollector"); sub != nil {
		err := sub.UnmarshalExact(capacity)
		if err != nil {
			return *capacity, err
		}
		return *capacity, nil
	}
	return *capacity, errors.New("No config found for inMemCollector")
}

func (f *fileConfig) GetMetricsType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.Metrics, nil
}

func (f *fileConfig) GetHoneycombMetricsConfig() (HoneycombMetricsConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	hmConfig := &HoneycombMetricsConfig{}
	if sub := f.config.Sub("HoneycombMetrics"); sub != nil {
		err := sub.UnmarshalExact(hmConfig)
		if err != nil {
			return *hmConfig, err
		}

		v := validator.New()
		err = v.Struct(hmConfig)
		if err != nil {
			return *hmConfig, err
		}

		return *hmConfig, nil
	}
	return *hmConfig, errors.New("No config found for HoneycombMetrics")
}

func (f *fileConfig) GetPrometheusMetricsConfig() (PrometheusMetricsConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	pcConfig := &PrometheusMetricsConfig{}
	if sub := f.config.Sub("PrometheusMetrics"); sub != nil {
		err := sub.UnmarshalExact(pcConfig)
		if err != nil {
			return *pcConfig, err
		}

		v := validator.New()
		err = v.Struct(pcConfig)
		if err != nil {
			return *pcConfig, err
		}

		return *pcConfig, nil
	}
	return *pcConfig, errors.New("No config found for PrometheusMetrics")
}

func (f *fileConfig) GetSendDelay() (time.Duration, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.SendDelay, nil
}

func (f *fileConfig) GetTraceTimeout() (time.Duration, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.TraceTimeout, nil
}

func (f *fileConfig) GetOtherConfig(name string, iface interface{}) error {
	f.mux.RLock()
	defer f.mux.RUnlock()

	if sub := f.config.Sub(name); sub != nil {
		return sub.Unmarshal(iface)
	}

	if sub := f.rules.Sub(name); sub != nil {
		return sub.Unmarshal(iface)
	}

	return fmt.Errorf("failed to find config tree for %s", name)
}

func (f *fileConfig) GetUpstreamBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.UpstreamBufferSize
}

func (f *fileConfig) GetPeerBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.PeerBufferSize
}

func (f *fileConfig) GetSendTickerValue() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.SendTicker
}

func (f *fileConfig) GetDebugServiceAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.conf.DebugServiceAddr)
	if err != nil {
		return "", err
	}
	return f.conf.DebugServiceAddr, nil
}

func (f *fileConfig) GetIsDryRun() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.DryRun
}

func (f *fileConfig) GetDryRunFieldName() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.conf.DryRunFieldName
}
