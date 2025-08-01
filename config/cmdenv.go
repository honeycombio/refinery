package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/jessevdk/go-flags"
)

// CmdEnv is a struct that contains all the command line options; it's separate
// from the config struct so that we can apply the command line options and env
// vars after loading the config, and so they don't have to be tied to the
// config struct. Command line options override env vars, and both of them
// override values already in the struct when ApplyCmdEnvTags is called.
//
// There are few circumstances where an env should not be specified -- basically
// only the bools that take an action and immediately exit.
//
// With the exception of the Config locations, default values should be
// specified in the config, because any nonzero default value here will override
// a value read in from the config.
//
// If description is specified, it is printed as part of the help text. Note
// that this system uses reflection to establish the relationship between the
// config struct and the command line options.
type CmdEnv struct {
	ConfigLocations                     []string          `short:"c" long:"config" env:"REFINERY_CONFIG" env-delim:"," default:"/etc/refinery/refinery.yaml" description:"config file or URL to load; can be specified more than once"`
	RulesLocations                      []string          `short:"r" long:"rules_config" env:"REFINERY_RULES_CONFIG" env-delim:"," default:"/etc/refinery/rules.yaml" description:"config file or URL to load; can be specified more than once"`
	HTTPListenAddr                      string            `long:"http-listen-address" env:"REFINERY_HTTP_LISTEN_ADDRESS" description:"HTTP listen address for incoming event traffic"`
	PeerListenAddr                      string            `long:"peer-listen-address" env:"REFINERY_PEER_LISTEN_ADDRESS" description:"Peer listen address for communication between Refinery instances"`
	GRPCListenAddr                      string            `long:"grpc-listen-address" env:"REFINERY_GRPC_LISTEN_ADDRESS" description:"gRPC listen address for OTLP traffic"`
	RedisHost                           string            `long:"redis-host" env:"REFINERY_REDIS_HOST" description:"Redis host address"`
	RedisClusterHosts                   []string          `long:"redis-cluster-hosts" env:"REFINERY_REDIS_CLUSTER_HOSTS" env-delim:"," description:"Redis cluster host addresses"`
	RedisUsername                       string            `long:"redis-username" env:"REFINERY_REDIS_USERNAME" description:"Redis username. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	RedisPassword                       string            `long:"redis-password" env:"REFINERY_REDIS_PASSWORD" description:"Redis password. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	RedisAuthCode                       string            `long:"redis-auth-code" env:"REFINERY_REDIS_AUTH_CODE" description:"Redis AUTH code. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	HoneycombAPI                        string            `long:"honeycomb-api" env:"REFINERY_HONEYCOMB_API" description:"Honeycomb API URL"`
	HoneycombAPIKey                     string            `long:"honeycomb-api-key" env:"REFINERY_HONEYCOMB_API_KEY" description:"Honeycomb API key (for logger and metrics). Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	HoneycombLoggerAPIKey               string            `long:"logger-api-key" env:"REFINERY_HONEYCOMB_LOGGER_API_KEY" description:"Honeycomb logger API key. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	HoneycombLoggerAdditionalAttributes map[string]string `long:"logger-additional-attributes" env:"REFINERY_HONEYCOMB_LOGGER_ADDITIONAL_ATTRIBUTES" env-delim:"," description:"Additional attributes to add to all logs written by the Honeycomb logger. When supplying via a environment variable, the value should be a string of comma-separated key-value pairs. When supplying via the command line, the value should be a key value pair. If multiple key-value pairs are needed, each should be supplied via its own command line flag. The key-value pairs must use ':' as the separator."`
	LegacyMetricsAPIKey                 string            `long:"legacy-metrics-api-key" env:"REFINERY_HONEYCOMB_METRICS_API_KEY" description:"API key for legacy Honeycomb metrics. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	OpAMPEndpoint                       string            `long:"opamp-server-url" env:"REFINERY_OPAMP_ENDPOINT" description:"URL of the OpAMP server to use for remote management."`
	TelemetryEndpoint                   string            `long:"telemetry-endpoint" env:"REFINERY_TELEMETRY_ENDPOINT" description:"Endpoint to send Refinery's internal telemetry to. This is separate from the Honeycomb API endpoint and is used for sending metrics about Refinery's performance."`
	OTelMetricsAPIKey                   string            `long:"otel-metrics-api-key" env:"REFINERY_OTEL_METRICS_API_KEY" description:"API key for OTel metrics if being sent to Honeycomb. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	OTelTracesAPIKey                    string            `long:"otel-traces-api-key" env:"REFINERY_OTEL_TRACES_API_KEY" description:"API key for OTel traces if being sent to Honeycomb. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	QueryAuthToken                      string            `long:"query-auth-token" env:"REFINERY_QUERY_AUTH_TOKEN" description:"Token for debug/management queries. Setting this value via a flag may expose credentials - it is recommended to use the env var or a configuration file."`
	AvailableMemory                     MemorySize        `long:"available-memory" env:"REFINERY_AVAILABLE_MEMORY" description:"The maximum memory available for Refinery to use (ex: 4GiB)."`
	SendKey                             string            `long:"send-key" env:"REFINERY_SEND_KEY" description:"The Honeycomb API key that Refinery can use to send data to Honeycomb."`
	Debug                               bool              `short:"d" long:"debug" description:"Runs debug service (on the first open port between localhost:6060 and :6069 by default)"`
	Version                             bool              `short:"v" long:"version" description:"Print version number and exit"`
	InterfaceNames                      bool              `long:"interface-names" description:"Print system's network interface names and exit."`
	Validate                            bool              `short:"V" long:"validate" description:"Validate the configuration files, writing results to stdout, and exit with 0 if valid, 1 if invalid."`
	NoValidate                          bool              `long:"no-validate" description:"Do not attempt to validate the configuration files. Makes --validate meaningless."`
	WriteConfig                         string            `long:"write-config" description:"After applying defaults, environment variables, and command line values, write the loaded configuration to the specified file as YAML and exit."`
	WriteRules                          string            `long:"write-rules" description:"After applying defaults, write the loaded rules to the specified file as YAML and exit."`
}

func NewCmdEnvOptions(args []string) (*CmdEnv, error) {
	opts := &CmdEnv{}

	if args == nil {
		args = os.Args
	}
	if _, err := flags.ParseArgs(opts, args); err != nil {
		switch flagsErr := err.(type) {
		case *flags.Error:
			if flagsErr.Type == flags.ErrHelp {
				os.Exit(0)
			}
			return nil, err
		default:
			return nil, err
		}
	}

	return opts, nil
}

// GetField returns the reflect.Value for the field with the given name in the CmdEnvOptions struct.
func (c *CmdEnv) GetField(name string) reflect.Value {
	return reflect.ValueOf(c).Elem().FieldByName(name)
}

func (c *CmdEnv) GetDelimiter(name string) string {
	field, ok := reflect.TypeOf(c).Elem().FieldByName(name)
	if !ok {
		return ""
	}
	return field.Tag.Get("env-delim")
}

// ApplyTags uses reflection to apply the values from the CmdEnv struct to the
// given struct. Any field in the struct that wants to be set from the command
// line must have a `cmdenv` tag on it that names one or more fields in the
// CmdEnv struct that should be used to set the value; the first one specified
// that has a value is used. The types must match. If the name field in CmdEnv
// field is the zero value, then it will not be applied.
func (c *CmdEnv) ApplyTags(s reflect.Value) error {
	return applyCmdEnvTags(s, c)
}

type getFielder interface {
	GetField(name string) reflect.Value
	GetDelimiter(name string) string
}

// applyCmdEnvTags is a helper function that applies the values from the given
// GetFielder to the given struct. We do it this way to make it easier to test.
func applyCmdEnvTags(s reflect.Value, fielder getFielder) error {
	switch s.Kind() {
	case reflect.Struct:
		t := s.Type()

		for i := 0; i < s.NumField(); i++ {
			field := s.Field(i)
			fieldType := t.Field(i)

			if tags := fieldType.Tag.Get("cmdenv"); tags != "" {
				// this field has a cmdenv tag, so try all its values
				for _, tag := range strings.Split(tags, ",") {
					value := fielder.GetField(tag)
					if !value.IsValid() {
						// if you get this error, you didn't specify cmdenv tags
						// correctly -- its value must be the name of a field in the struct
						return fmt.Errorf("programming error -- invalid field name: %s", tag)
					}
					if !field.CanSet() {
						return fmt.Errorf("programming error -- cannot set new value for: %s", fieldType.Name)
					}

					// don't overwrite values that are already set
					if value.IsZero() || ((value.Kind() == reflect.Map || value.Kind() == reflect.Slice) && value.Len() == 0) {
						continue
					}

					// ensure that the types match
					if fieldType.Type != value.Type() {
						return fmt.Errorf("programming error -- types don't match for field: %s (%v and %v)",
							fieldType.Name, fieldType.Type, value.Type())
					}

					if value.Kind() == reflect.Slice {
						delimiter := fielder.GetDelimiter(tag)
						if delimiter == "" {
							return fmt.Errorf("programming error -- missing delimiter for slice field: %s", fieldType.Name)
						}

						rawValue, ok := value.Index(0).Interface().(string)
						if !ok {
							return fmt.Errorf("programming error -- slice field must be a string: %s", fieldType.Name)
						}

						// split the value on the delimiter
						values := strings.Split(rawValue, delimiter)
						// create a new slice of the same type as the field
						slice := reflect.MakeSlice(field.Type(), len(values), len(values))
						// iterate over the values and set them
						for i, v := range values {
							slice.Index(i).SetString(v)
						}
						// set the field
						field.Set(slice)
						break
					}
					// now we can set it
					field.Set(value)
					// and we're done with this field
					break
				}
			}

			// recurse into any nested structs
			err := applyCmdEnvTags(field, fielder)
			if err != nil {
				return err
			}
		}

	case reflect.Ptr:
		if !s.IsNil() {
			return applyCmdEnvTags(s.Elem(), fielder)
		}
	}
	return nil
}
