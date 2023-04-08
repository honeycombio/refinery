package config

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

type fileConfig struct {
	// config        *viper.Viper
	// rules         *viper.Viper
	mainConfig    *configContents
	rulesConfig   *rulesContents
	opts          *CmdEnv
	callbacks     []func()
	errorCallback func(error)
	mux           sync.RWMutex
	lastLoadTime  time.Time
}

type configContents struct {
	ListenAddr                string                         `default:"0.0.0.0:8080" cmdenv:"HTTPListenAddr" validate:"required"`
	PeerListenAddr            string                         `default:"0.0.0.0:8081" cmdenv:"PeerListenAddr" validate:"required"`
	CompressPeerCommunication bool                           `default:"true"`
	GRPCListenAddr            string                         `cmdenv:"GRPCListenAddr"`
	APIKeys                   []string                       `default:"[\"*\"]" validate:"required"`
	HoneycombAPI              string                         `default:"https://api.honeycomb.io" cmdenv:"HoneycombAPI" validate:"required,url"`
	Logger                    string                         `default:"logrus" validate:"required,oneof= logrus honeycomb"`
	LoggingLevel              string                         `default:"info" validate:"required,oneof= debug info warn error"`
	Collector                 string                         `default:"InMemCollector" validate:"required,oneof= InMemCollector"`
	Metrics                   string                         `default:"honeycomb" validate:"required,oneof= prometheus honeycomb"`
	SendDelay                 time.Duration                  `default:"2s" validate:"required"`
	BatchTimeout              time.Duration                  `default:"100ms"`
	TraceTimeout              time.Duration                  `default:"60s" validate:"required"`
	MaxBatchSize              uint                           `default:"500" validate:"required"`
	SendTicker                time.Duration                  `default:"100ms" validate:"required"`
	UpstreamBufferSize        int                            `default:"10_000" validate:"required"`
	PeerBufferSize            int                            `default:"10_000" validate:"required"`
	DebugServiceAddr          string                         ``
	PeerManagement            PeerManagementConfig           `validate:"required"`
	InMemCollector            InMemoryCollectorCacheCapacity `validate:"required"`
	AddHostMetadataToTrace    bool                           ``
	AddRuleReasonToTrace      bool                           ``
	EnvironmentCacheTTL       time.Duration                  `default:"1h"`
	DatasetPrefix             string                         ``
	QueryAuthToken            string                         `cmdenv:"QueryAuthToken"`
	AdditionalErrorFields     []string                       `default:"[\"trace.span_id\"]"`
	AddSpanCountToRoot        bool                           ``
	CacheOverrunStrategy      string                         `default:"impact"`
	SampleCache               SampleCacheConfig              `validate:"required"`
	StressRelief              StressReliefConfig             `validate:"required"`
	AdditionalAttributes      map[string]string              `default:"{}"`
	TraceIdFieldNames         []string                       `default:"[\"trace.trace_id\",\"traceId\"]"`
	ParentIdFieldNames        []string                       `default:"[\"trace.parent_id\",\"parentId\"]"`
	GRPCServerParameters      *GRPCServerParameters
	HoneycombLogger           *HoneycombLoggerConfig
	HoneycombMetrics          *HoneycombMetricsConfig
	PrometheusMetrics         *PrometheusMetricsConfig
}

type rulesContents struct {
	Sampler         string `default:"DeterministicSampler" validate:"required,oneof= DeterministicSampler DynamicSampler EMADynamicSampler RulesBasedSampler TotalThroughputSampler"`
	DryRun          bool   ``
	DryRunFieldName string `default:"refinery_kept"`
}

type InMemoryCollectorCacheCapacity struct {
	// CacheCapacity must be less than math.MaxInt32
	CacheCapacity int    `default:"10_000" validate:"required,lt=2147483647"`
	MaxAlloc      uint64 ``
}

type HoneycombLevel int

type HoneycombLoggerConfig struct {
	LoggerHoneycombAPI      string         `validate:"required,url"`
	LoggerAPIKey            string         `cmdenv:"HoneycombLoggerAPIKey,HoneycombAPIKey" validate:"required"`
	LoggerDataset           string         `default:"Refinery Logs" validate:"required"`
	LoggerSamplerEnabled    bool           ``
	LoggerSamplerThroughput int            ``
	Level                   HoneycombLevel `default:"Warn"`
}

type PrometheusMetricsConfig struct {
	MetricsListenAddr string `validate:"required"`
}

type HoneycombMetricsConfig struct {
	MetricsHoneycombAPI      string `validate:"required,url"`
	MetricsAPIKey            string `cmdenv:"HoneycombMetricsAPIKey,HoneycombAPIKey" validate:"required"`
	MetricsDataset           string `validate:"required"`
	MetricsReportingInterval int64  `default:"3s" validate:"required"`
}

type PeerManagementConfig struct {
	Type                    string        `default:"file" validate:"required,oneof= file redis"`
	Peers                   []string      `default:"[\"http://127.0.0.1:8081\"]" validate:"dive,url"`
	RedisHost               string        `cmdenv:"RedisHost"`
	RedisUsername           string        `cmdenv:"RedisUsername"`
	RedisPassword           string        `cmdenv:"RedisPassword"`
	RedisPrefix             string        `default:"refinery" validate:"required"`
	RedisDatabase           int           `validate:"gte=0,lte=15"`
	RedisIdentifier         string        ``
	UseTLS                  bool          ``
	UseTLSInsecure          bool          ``
	IdentifierInterfaceName string        ``
	UseIPV6Identifier       bool          ``
	Timeout                 time.Duration `default:"5s" validate:"gte=1_000_000_000"`
	Strategy                string        `default:"legacy" validate:"required,oneof= legacy hash"`
}

type SampleCacheConfig struct {
	Type              string        `default:"legacy" validate:"required,oneof= legacy cuckoo"`
	KeptSize          uint          `default:"10_000" validate:"gte=500"`
	DroppedSize       uint          `default:"1_000_000" validate:"gte=100_000"`
	SizeCheckInterval time.Duration `default:"10s" validate:"gte=1_000_000_000"` // 1 second minimum
}

// GRPCServerParameters allow you to configure the GRPC ServerParameters used
// by refinery's own GRPC server:
// https://pkg.go.dev/google.golang.org/grpc/keepalive#ServerParameters
type GRPCServerParameters struct {
	MaxConnectionIdle     time.Duration `default:"1s"`
	MaxConnectionAge      time.Duration `default:"5s"`
	MaxConnectionAgeGrace time.Duration `default:"3s"`
	Time                  time.Duration `default:"10s"`
	Timeout               time.Duration `default:"2s"`
}

type StressReliefConfig struct {
	Mode                      string        `default:"never" validate:"required,oneof= always never monitor"`
	ActivationLevel           uint          `default:"90" validate:"gte=0,lte=100"`
	DeactivationLevel         uint          `default:"75" validate:"gte=0,lte=100"`
	StressSamplingRate        uint64        `default:"1000" validate:"gte=1"`
	MinimumActivationDuration time.Duration `default:"10s"`
	StartStressedDuration     time.Duration `default:"3s"`
}

// NewConfig creates a new Config object from the given arguments; if args is
// nil, it uses the command line arguments
func NewConfig(opts *CmdEnv, errorCallback func(error)) (Config, error) {
	mainconf := &configContents{}
	err := readConfigInto(mainconf, opts.ConfigLocation, opts)
	if err != nil {
		return nil, err
	}

	rulesconf := &rulesContents{}
	err = readConfigInto(rulesconf, opts.RulesLocation, opts)
	if err != nil {
		return nil, err
	}

	// TODO: these callbacks aren't called because the new system doesn't
	// yet monitor for changes. It will in a future PR.
	cfg := &fileConfig{
		mainConfig:    mainconf,
		rulesConfig:   rulesconf,
		opts:          opts,
		callbacks:     make([]func(), 0),
		errorCallback: errorCallback,
	}

	return cfg, nil
}

// // NewConfig creates a new config struct
// func NewConfig(config, rules string, errorCallback func(error)) (Config, error) {
// 	c := viper.New()

// 	c.BindEnv("GRPCListenAddr", "REFINERY_GRPC_LISTEN_ADDRESS")
// 	c.BindEnv("PeerManagement.RedisHost", "REFINERY_REDIS_HOST")
// 	c.BindEnv("PeerManagement.RedisUsername", "REFINERY_REDIS_USERNAME")
// 	c.BindEnv("PeerManagement.RedisPassword", "REFINERY_REDIS_PASSWORD")
// 	c.BindEnv("HoneycombLogger.LoggerAPIKey", "REFINERY_HONEYCOMB_API_KEY")
// 	c.BindEnv("HoneycombMetrics.MetricsAPIKey", "REFINERY_HONEYCOMB_METRICS_API_KEY", "REFINERY_HONEYCOMB_API_KEY")
// 	c.BindEnv("QueryAuthToken", "REFINERY_QUERY_AUTH_TOKEN")
// 	c.SetDefault("ListenAddr", "0.0.0.0:8080")
// 	c.SetDefault("PeerListenAddr", "0.0.0.0:8081")
// 	c.SetDefault("CompressPeerCommunication", true)
// 	c.SetDefault("APIKeys", []string{"*"})
// 	c.SetDefault("PeerManagement.Peers", []string{"http://127.0.0.1:8081"})
// 	c.SetDefault("PeerManagement.RedisPrefix", "refinery")
// 	c.SetDefault("PeerManagement.Type", "file")
// 	c.SetDefault("PeerManagement.UseTLS", false)
// 	c.SetDefault("PeerManagement.UseTLSInsecure", false)
// 	c.SetDefault("PeerManagement.UseIPV6Identifier", false)
// 	c.SetDefault("PeerManagement.Timeout", 5*time.Second)
// 	c.SetDefault("PeerManagement.Strategy", "legacy")
// 	c.SetDefault("HoneycombAPI", "https://api.honeycomb.io")
// 	c.SetDefault("Logger", "logrus")
// 	c.SetDefault("LoggingLevel", "debug")
// 	c.SetDefault("Collector", "InMemCollector")
// 	c.SetDefault("Metrics", "honeycomb")
// 	c.SetDefault("SendDelay", 2*time.Second)
// 	c.SetDefault("BatchTimeout", libhoney.DefaultBatchTimeout)
// 	c.SetDefault("TraceTimeout", 60*time.Second)
// 	c.SetDefault("MaxBatchSize", 500)
// 	c.SetDefault("SendTicker", 100*time.Millisecond)
// 	c.SetDefault("UpstreamBufferSize", libhoney.DefaultPendingWorkCapacity)
// 	c.SetDefault("PeerBufferSize", libhoney.DefaultPendingWorkCapacity)
// 	c.SetDefault("MaxAlloc", uint64(0))
// 	c.SetDefault("HoneycombLogger.LoggerSamplerEnabled", false)
// 	c.SetDefault("HoneycombLogger.LoggerSamplerThroughput", 5)
// 	c.SetDefault("AddHostMetadataToTrace", false)
// 	c.SetDefault("AddRuleReasonToTrace", false)
// 	c.SetDefault("EnvironmentCacheTTL", time.Hour)
// 	c.SetDefault("GRPCServerParameters.MaxConnectionIdle", 1*time.Minute)
// 	c.SetDefault("GRPCServerParameters.MaxConnectionAge", time.Duration(0))
// 	c.SetDefault("GRPCServerParameters.MaxConnectionAgeGrace", time.Duration(0))
// 	c.SetDefault("GRPCServerParameters.Time", 10*time.Second)
// 	c.SetDefault("GRPCServerParameters.Timeout", 2*time.Second)
// 	c.SetDefault("AdditionalErrorFields", []string{"trace.span_id"})
// 	c.SetDefault("AddSpanCountToRoot", false)
// 	c.SetDefault("CacheOverrunStrategy", "resize")
// 	c.SetDefault("SampleCache.Type", "legacy")
// 	c.SetDefault("SampleCache.KeptSize", 10_000)
// 	c.SetDefault("SampleCache.DroppedSize", 1_000_000)
// 	c.SetDefault("SampleCache.SizeCheckInterval", 10*time.Second)
// 	c.SetDefault("StressRelief.Mode", "never")
// 	c.SetDefault("StressRelief.ActivationLevel", 75)
// 	c.SetDefault("StressRelief.DeactivationLevel", 25)
// 	c.SetDefault("StressRelief.StressSamplingRate", 100)
// 	c.SetDefault("StressRelief.MinimumActivationDuration", 10*time.Second)
// 	c.SetDefault("StressRelief.StartStressedDuration", 3*time.Second)
// 	c.SetDefault("AdditionalAttributes", make(map[string]string))
// 	c.SetDefault("TraceIdFieldNames", []string{"trace.trace_id", "traceId"})
// 	c.SetDefault("ParentIdFieldNames", []string{"trace.parent_id", "parentId"})

// 	c.SetConfigFile(config)
// 	err := c.ReadInConfig()

// 	if err != nil {
// 		return nil, err
// 	}

// 	r := viper.New()

// 	r.SetDefault("Sampler", "DeterministicSampler")
// 	r.SetDefault("SampleRate", 1)
// 	r.SetDefault("DryRun", false)
// 	r.SetDefault("DryRunFieldName", "refinery_kept")

// 	r.SetConfigFile(rules)
// 	err = r.ReadInConfig()

// 	if err != nil {
// 		return nil, err
// 	}

// 	fc := &fileConfig{
// 		config:        c,
// 		rules:         r,
// 		conf:          &configContents{},
// 		callbacks:     make([]func(), 0),
// 		errorCallback: errorCallback,
// 	}

// 	err = fc.unmarshal()

// 	if err != nil {
// 		return nil, err
// 	}

// 	v := validator.New()
// 	err = v.Struct(fc.conf)
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = fc.validateGeneralConfigs()
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = fc.validateSamplerConfigs()
// 	if err != nil {
// 		return nil, err
// 	}

// 	c.WatchConfig()
// 	c.OnConfigChange(fc.onChange)

// 	r.WatchConfig()
// 	r.OnConfigChange(fc.onChange)

// 	return fc, nil
// }

// func (f *fileConfig) onChange(in fsnotify.Event) {
// 	v := validator.New()
// 	err := v.Struct(f.conf)
// 	if err != nil {
// 		f.errorCallback(err)
// 		return
// 	}

// 	err = f.validateGeneralConfigs()
// 	if err != nil {
// 		f.errorCallback(err)
// 		return
// 	}

// 	err = f.validateSamplerConfigs()
// 	if err != nil {
// 		f.errorCallback(err)
// 		return
// 	}

// 	f.unmarshal()

// 	for _, c := range f.callbacks {
// 		c()
// 	}
// }

// func (f *fileConfig) unmarshal() error {
// 	f.mux.Lock()
// 	defer f.mux.Unlock()
// 	err := f.config.Unmarshal(f.conf)

// 	if err != nil {
// 		return err
// 	}

// 	err = f.rulesConfig.Unmarshal(f.conf)

// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (f *fileConfig) validateGeneralConfigs() error {
// 	f.lastLoadTime = time.Now()

// 	// validate logger config
// 	loggerType, err := f.GetLoggerType()
// 	if err != nil {
// 		return err
// 	}
// 	if loggerType == "honeycomb" {
// 		_, err = f.GetHoneycombLoggerConfig()
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	// validate metrics config
// 	metricsType, err := f.GetMetricsType()
// 	if err != nil {
// 		return err
// 	}
// 	if metricsType == "honeycomb" {
// 		_, err = f.GetHoneycombMetricsConfig()
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	if metricsType == "prometheus" {
// 		_, err = f.GetPrometheusMetricsConfig()
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	// validate cache strategy
// 	st := f.GetCacheOverrunStrategy()
// 	switch st {
// 	case "resize", "impact":
// 		break
// 	default:
// 		return fmt.Errorf("invalid CacheOverrunStrategy: '%s'", st)
// 	}
// 	return nil
// }

// func (f *fileConfig) validateSamplerConfigs() error {
// 	logrus.Debugf("Sampler rules config: %+v", f.rules)

// 	keys := f.rulesConfig.AllKeys()
// 	for _, key := range keys {
// 		parts := strings.Split(key, ".")

// 		// verify default sampler config
// 		if parts[0] == "sampler" {
// 			t := f.rulesConfig.GetStringey)
// 			var i interface{}
// 			switch t {
// 			case "DeterministicSampler":
// 				i = &DeterministicSamplerConfig{}
// 			case "DynamicSampler":
// 				i = &DynamicSamplerConfig{}
// 			case "EMADynamicSampler":
// 				i = &EMADynamicSamplerConfig{}
// 			case "RulesBasedSampler":
// 				i = &RulesBasedSamplerConfig{}
// 			case "TotalThroughputSampler":
// 				i = &TotalThroughputSamplerConfig{}
// 			default:
// 				return fmt.Errorf("Invalid or missing default sampler type: %s", t)
// 			}
// 			err := f.rulesConfig.Unmarshal(i)
// 			if err != nil {
// 				return fmt.Errorf("Failed to unmarshal sampler rule: %w", err)
// 			}
// 			v := validator.New()
// 			err = v.Struct(i)
// 			if err != nil {
// 				return fmt.Errorf("Failed to validate sampler rule: %w", err)
// 			}
// 		}

// 		// verify dataset sampler configs
// 		if len(parts) > 1 && parts[1] == "sampler" {
// 			t := f.rulesConfig.GetStringey)
// 			var i interface{}
// 			switch t {
// 			case "DeterministicSampler":
// 				i = &DeterministicSamplerConfig{}
// 			case "DynamicSampler":
// 				i = &DynamicSamplerConfig{}
// 			case "EMADynamicSampler":
// 				i = &EMADynamicSamplerConfig{}
// 			case "RulesBasedSampler":
// 				i = &RulesBasedSamplerConfig{}
// 			case "TotalThroughputSampler":
// 				i = &TotalThroughputSamplerConfig{}
// 			default:
// 				return fmt.Errorf("Invalid or missing dataset sampler type: %s", t)
// 			}
// 			datasetName := parts[0]
// 			if sub := f.rulesConfig.Sub(datasetName); sub != nil {
// 				err := sub.Unmarshal(i)
// 				if err != nil {
// 					return fmt.Errorf("Failed to unmarshal dataset sampler rule: %w", err)
// 				}
// 				v := validator.New()
// 				err = v.Struct(i)
// 				if err != nil {
// 					return fmt.Errorf("Failed to validate dataset sampler rule: %w", err)
// 				}
// 			}
// 		}
// 	}
// 	return nil
// }

func (f *fileConfig) RegisterReloadCallback(cb func()) {
	f.mux.Lock()
	defer f.mux.Unlock()

	f.callbacks = append(f.callbacks, cb)
}

func (f *fileConfig) GetListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.ListenAddr)
	if err != nil {
		return "", err
	}
	return f.mainConfig.ListenAddr, nil
}

func (f *fileConfig) GetPeerListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.PeerListenAddr)
	if err != nil {
		return "", err
	}
	return f.mainConfig.PeerListenAddr, nil
}

func (f *fileConfig) GetCompressPeerCommunication() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.CompressPeerCommunication
}

func (f *fileConfig) GetGRPCListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	// GRPC listen addr is optional, only check value is valid if not empty
	if f.mainConfig.GRPCListenAddr != "" {
		_, _, err := net.SplitHostPort(f.mainConfig.GRPCListenAddr)
		if err != nil {
			return "", err
		}
	}
	return f.mainConfig.GRPCListenAddr, nil
}

func (f *fileConfig) GetAPIKeys() ([]string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.APIKeys, nil
}

func (f *fileConfig) GetPeerManagementType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Type, nil
}

func (f *fileConfig) GetPeerManagementStrategy() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Strategy, nil
}

func (f *fileConfig) GetPeers() ([]string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Peers, nil
}

func (f *fileConfig) GetRedisHost() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisHost, nil
}

func (f *fileConfig) GetRedisUsername() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisUsername, nil
}

func (f *fileConfig) GetRedisPrefix() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisPrefix
}

func (f *fileConfig) GetRedisPassword() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisPassword, nil
}

func (f *fileConfig) GetRedisDatabase() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisDatabase
}

func (f *fileConfig) GetUseTLS() (bool, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.UseTLS, nil
}

func (f *fileConfig) GetUseTLSInsecure() (bool, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.UseTLSInsecure, nil
}

func (f *fileConfig) GetIdentifierInterfaceName() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.IdentifierInterfaceName, nil
}

func (f *fileConfig) GetUseIPV6Identifier() (bool, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.UseIPV6Identifier, nil
}

func (f *fileConfig) GetRedisIdentifier() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisIdentifier, nil
}

func (f *fileConfig) GetHoneycombAPI() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.HoneycombAPI, nil
}

func (f *fileConfig) GetLoggingLevel() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.LoggingLevel, nil
}

func (f *fileConfig) GetLoggerType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Logger, nil
}

func (f *fileConfig) GetHoneycombLoggerConfig() (HoneycombLoggerConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return *f.mainConfig.HoneycombLogger, nil
}

func (f *fileConfig) GetCollectorType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Collector, nil
}

func (f *fileConfig) GetAllSamplerRules() (map[string]interface{}, error) {
	samplers := make(map[string]interface{})

	err := readConfigInto(samplers, f.opts.RulesLocation, f.opts)
	if err != nil {
		return nil, err
	}

	// This is probably good enough for debug; if not we can extend it.
	return samplers, nil
}

// getValueForCaseInsensitiveKey is a generic function that returns the value from a map[string]interface{}
// for the given key, ignoring case of the key. It returns ok=true only if the key was found
// and could be converted to the required type.
func getValueForCaseInsensitiveKey[T any](m map[string]interface{}, key string) (T, bool) {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			if t, ok := v.(T); ok {
				return t, true
			}
		}
	}
	var zero T
	return zero, false
}

// reloadInto accepts a map[string]any and a struct, and loads the map into the struct
// by re-marshalling the map into JSON and then unmarshalling the JSON into the struct.
func reloadInto(m map[string]any, s interface{}) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, s)
}

func (f *fileConfig) GetSamplerConfigForDataset(dataset string) (any, string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	config := make(map[string]any)

	err := readConfigInto(config, f.opts.RulesLocation, f.opts)
	if err != nil {
		return nil, "", err
	}

	// If we have a dataset-specific sampler, we extract the sampler config
	// corresponding to the [dataset]["sampler"] key. Otherwise we try to use
	// the default sampler config corresponding to the "sampler" key. Only if
	// both fail will we return not found.

	const notfound = "not found"
	if v, ok := getValueForCaseInsensitiveKey[map[string]any](config, dataset); ok {
		// we have a dataset-specific sampler, so we extract that sampler's config
		config = v
	}

	// now we need the name of the sampler
	samplerName, _ := getValueForCaseInsensitiveKey[string](config, "sampler")

	var i any
	switch samplerName {
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
		return nil, notfound, errors.New("no sampler found")
	}

	// now we need to unmarshal the config into the sampler config struct
	err = reloadInto(config, i)
	return i, samplerName, err
}

func (f *fileConfig) GetInMemCollectorCacheCapacity() (InMemoryCollectorCacheCapacity, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.InMemCollector, nil
}

func (f *fileConfig) GetMetricsType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Metrics, nil
}

func (f *fileConfig) GetHoneycombMetricsConfig() (HoneycombMetricsConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return *f.mainConfig.HoneycombMetrics, nil
}

func (f *fileConfig) GetPrometheusMetricsConfig() (PrometheusMetricsConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return *f.mainConfig.PrometheusMetrics, nil
}

func (f *fileConfig) GetSendDelay() (time.Duration, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.SendDelay, nil
}

func (f *fileConfig) GetBatchTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.BatchTimeout
}

func (f *fileConfig) GetTraceTimeout() (time.Duration, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.TraceTimeout, nil
}

func (f *fileConfig) GetMaxBatchSize() uint {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.MaxBatchSize
}

func (f *fileConfig) GetUpstreamBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.UpstreamBufferSize
}

func (f *fileConfig) GetPeerBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerBufferSize
}

func (f *fileConfig) GetSendTickerValue() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.SendTicker
}

func (f *fileConfig) GetDebugServiceAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.DebugServiceAddr)
	if err != nil {
		return "", err
	}
	return f.mainConfig.DebugServiceAddr, nil
}

func (f *fileConfig) GetIsDryRun() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.rulesConfig.DryRun
}

func (f *fileConfig) GetDryRunFieldName() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.rulesConfig.DryRunFieldName
}

func (f *fileConfig) GetAddHostMetadataToTrace() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AddHostMetadataToTrace
}

func (f *fileConfig) GetAddRuleReasonToTrace() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AddRuleReasonToTrace
}

func (f *fileConfig) GetEnvironmentCacheTTL() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.EnvironmentCacheTTL
}

func (f *fileConfig) GetDatasetPrefix() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.DatasetPrefix
}

func (f *fileConfig) GetQueryAuthToken() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.QueryAuthToken
}

func (f *fileConfig) GetGRPCMaxConnectionIdle() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.GRPCServerParameters.MaxConnectionIdle
}

func (f *fileConfig) GetGRPCMaxConnectionAge() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.GRPCServerParameters.MaxConnectionAge
}

func (f *fileConfig) GetGRPCMaxConnectionAgeGrace() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.GRPCServerParameters.MaxConnectionAgeGrace
}

func (f *fileConfig) GetGRPCTime() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.GRPCServerParameters.Time
}

func (f *fileConfig) GetGRPCTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.GRPCServerParameters.Timeout
}

func (f *fileConfig) GetPeerTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Timeout
}

func (f *fileConfig) GetAdditionalErrorFields() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AdditionalErrorFields
}

func (f *fileConfig) GetAddSpanCountToRoot() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AddSpanCountToRoot
}

func (f *fileConfig) GetCacheOverrunStrategy() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.CacheOverrunStrategy
}

func (f *fileConfig) GetSampleCacheConfig() SampleCacheConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.SampleCache
}

func (f *fileConfig) GetStressReliefConfig() StressReliefConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.StressRelief
}

func (f *fileConfig) GetTraceIdFieldNames() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.TraceIdFieldNames
}

func (f *fileConfig) GetParentIdFieldNames() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.ParentIdFieldNames
}

// calculates an MD5 sum for a file that returns the same result as the md5sum command
func calcMD5For(location string) string {
	r, _, err := getReaderFor(location)
	if err != nil {
		return err.Error()
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return err.Error()
	}
	h := md5.New()
	if _, err := h.Write(data); err != nil {
		return err.Error()
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (f *fileConfig) GetConfigMetadata() []ConfigMetadata {
	ret := make([]ConfigMetadata, 2)
	ret[0] = ConfigMetadata{
		Type:     "config",
		ID:       f.opts.ConfigLocation,
		Hash:     calcMD5For(f.opts.ConfigLocation),
		LoadedAt: f.lastLoadTime.Format(time.RFC3339),
	}
	ret[1] = ConfigMetadata{
		Type:     "rules",
		ID:       f.opts.RulesLocation,
		Hash:     calcMD5For(f.opts.RulesLocation),
		LoadedAt: f.lastLoadTime.Format(time.RFC3339),
	}
	return ret
}

func (f *fileConfig) GetAdditionalAttributes() map[string]string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AdditionalAttributes
}
