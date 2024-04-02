package redis

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/gomodule/redigo/redis"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
)

var _ Client = &TestService{}

type TestService struct {
	Config  config.Config   `inject:""` // currently ignored by the test service
	Clock   clockwork.Clock `inject:""`
	Metrics metrics.Metrics `inject:"genericMetrics"`
	Service *miniredis.Miniredis
	pool    *redis.Pool
}

func (s *TestService) Start() error {
	r, err := miniredis.Run()
	if err != nil {
		return err
	}
	s.Service = r
	if s.Config == nil {
		s.Config = &config.MockConfig{}
	}
	if s.Clock == nil {
		s.Clock = clockwork.NewFakeClock()
	}
	if s.Metrics == nil {
		s.Metrics = &metrics.NullMetrics{}
	}

	s.Service.SetTime(s.Clock.Now())
	s.pool = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", s.Service.Addr())
		},
	}

	return nil
}

func (s *TestService) Stop() error {
	s.pool.Close()
	s.Service.Close()
	return nil
}

func (s *TestService) Get() Conn {
	return &DefaultConn{
		conn:    s.pool.Get(),
		Clock:   s.Clock,
		metrics: s.Metrics,
	}
}

func (s *TestService) NewScript(keyCount int, src string) Script {
	return &DefaultScript{
		script: redis.NewScript(keyCount, src),
	}
}

func (s *TestService) Stats() redis.PoolStats {
	return redis.PoolStats{}
}
