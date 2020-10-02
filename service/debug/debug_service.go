package debug

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"sync"
	"syscall"

	"github.com/honeycombio/refinery/config"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
	"github.com/sirupsen/logrus"
)

const addr = "localhost:6060"

// injectable debug service
type DebugService struct {
	mux     *http.ServeMux
	urls    []string
	expVars map[string]interface{}
	mutex   sync.RWMutex
	Config  config.Config
}

func (s *DebugService) Start() error {
	s.expVars = make(map[string]interface{})

	s.mux = http.NewServeMux()

	// Add to the mux but don't add an index entry.
	s.mux.HandleFunc("/", s.indexHandler)

	s.HandleFunc("/debug/pprof/", pprof.Index)
	s.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	s.HandleFunc("/debug/pprof/profile", pprof.Profile)
	s.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	s.HandleFunc("/debug/pprof/trace", pprof.Trace)
	s.HandleFunc("/debug/vars", s.expvarHandler)
	s.Handle("/debug/metrics", exp.ExpHandler(metrics.DefaultRegistry))
	s.Publish("cmdline", os.Args)
	s.Publish("memstats", Func(memstats))

	go func() {
		configAddr, _ := s.Config.GetDebugServiceAddr()
		if configAddr != "" {
			host, portStr, _ := net.SplitHostPort(configAddr)
			addr := net.JoinHostPort(host, portStr)
			logrus.Infof("Debug service listening on %s", addr)

			err := http.ListenAndServe(addr, s.mux)
			logrus.WithError(err).Warn("debug http server error")
		} else {
			// Prefer to listen on addr, but will try to bind to the next 9 ports
			// in case you have multiple services running on the same host.
			for i := 0; i < 10; i++ {
				host, portStr, _ := net.SplitHostPort(addr)
				port, _ := strconv.Atoi(portStr)
				port += i
				addr := net.JoinHostPort(host, fmt.Sprint(port))

				logrus.Infof("Debug service listening on %s", addr)

				err := http.ListenAndServe(addr, s.mux)
				logrus.WithError(err).Warn("debug http server error")

				if err, ok := err.(*net.OpError); ok {
					if err, ok := err.Err.(*os.SyscallError); ok {
						if err.Err == syscall.EADDRINUSE {
							// address already in use, try another
							continue
						}
					}
				}
				break
			}
		}
	}()

	return nil
}

// Use Handle and HandleFunc to add new services on the internal debugging port.
func (s *DebugService) Handle(pattern string, handler http.Handler) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.urls = append(s.urls, pattern)
	s.mux.Handle(pattern, handler)
}

func (s *DebugService) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.urls = append(s.urls, pattern)
	s.mux.HandleFunc(pattern, handler)
}

// Publish an expvar at /debug/vars, possibly using Func
func (s *DebugService) Publish(name string, v interface{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, existing := s.expVars[name]; existing {
		log.Panicln("Reuse of exported var name:", name)
	}
	s.expVars[name] = v
}

func (s *DebugService) indexHandler(w http.ResponseWriter, req *http.Request) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if err := indexTmpl.Execute(w, s.urls); err != nil {
		logrus.WithError(err).Warn("error rendering debug index")
	}
}

var indexTmpl = template.Must(template.New("index").Parse(`
<html>
<head>
<title>Debug Index</title>
</head>
<body>
<h2>Index</h2>
<table>
{{range .}}
<tr><td><a href="{{.}}?debug=1">{{.}}</a>
{{end}}
</table>
</body>
</html>
`))

func (s *DebugService) expvarHandler(w http.ResponseWriter, r *http.Request) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	values := make(map[string]interface{}, len(s.expVars))
	for k, v := range s.expVars {
		if f, ok := v.(Func); ok {
			v = f()
		}
		values[k] = v
	}
	b, err := json.MarshalIndent(values, "", "  ")
	if err != nil {
		logrus.WithError(err).Warn("error encoding expvars")
	}
	w.Write(b)
}

func memstats() interface{} {
	stats := new(runtime.MemStats)
	runtime.ReadMemStats(stats)
	return *stats
}

type Func func() interface{}
