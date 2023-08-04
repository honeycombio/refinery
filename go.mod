module github.com/honeycombio/refinery

go 1.20

require (
	github.com/agnivade/levenshtein v1.1.1
	github.com/creasty/defaults v1.7.0
	github.com/davecgh/go-spew v1.1.1
	github.com/dgryski/go-wyhash v0.0.0-20191203203029-c4841ae36371
	github.com/facebookgo/inject v0.0.0-20180706035515-f23751cae28b
	github.com/facebookgo/startstop v0.0.0-20161013234910-bc158412526d
	github.com/gomodule/redigo v1.8.9
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/honeycombio/dynsampler-go v0.5.1
	github.com/honeycombio/husky v0.22.4
	github.com/honeycombio/libhoney-go v1.20.0
	github.com/jessevdk/go-flags v1.5.0
	github.com/json-iterator/go v1.1.12
	github.com/klauspost/compress v1.16.7
	github.com/panmari/cuckoofilter v1.0.3
	github.com/pelletier/go-toml/v2 v2.0.9
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.16.0
	github.com/pyroscope-io/godeltaprof v0.1.1
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/sirupsen/logrus v1.9.3
	github.com/sourcegraph/conc v0.3.0
	github.com/stretchr/testify v1.8.4
	github.com/tidwall/gjson v1.15.0
	github.com/vmihailenco/msgpack/v4 v4.3.11
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v0.39.0
	go.opentelemetry.io/otel/metric v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
	go.opentelemetry.io/otel/sdk/metric v0.39.0
	go.opentelemetry.io/proto/otlp v0.19.0
	go.uber.org/automaxprocs v1.5.3
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29
	google.golang.org/grpc v1.57.0
	google.golang.org/protobuf v1.31.0
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-metro v0.0.0-20200812162917-85c65e2d0165 // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/facebookgo/limitgroup v0.0.0-20150612190941-6abd8d71ec01 // indirect
	github.com/facebookgo/muster v0.0.0-20150708232844-fd3d7953fd52 // indirect
	github.com/facebookgo/structtag v0.0.0-20150214074306-217e25fb9691 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.10.1 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser v0.1.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.16.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.39.0 // indirect
	go.opentelemetry.io/otel/trace v1.16.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/net v0.9.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230526161137-0005af68ea54 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20230525234035-dd9d682886f9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230525234030-28d5490b6b19 // indirect
)

replace go.opentelemetry.io/proto/otlp => github.com/honeycombio/opentelemetry-proto-go/otlp v0.19.0-compat

replace github.com/panmari/cuckoofilter => github.com/honeycombio/cuckoofilter v0.0.0-20230630225016-cf48793fb7c1
