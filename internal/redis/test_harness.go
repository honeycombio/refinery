package redis

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/gomodule/redigo/redis"
	"github.com/honeycombio/refinery/config"
	"github.com/jonboulle/clockwork"
)

var _ Client = &TestService{}

type TestService struct {
	Config  config.Config // currently ignored by the test service
	Clock   clockwork.Clock
	Service *miniredis.Miniredis
	pool    *redis.Pool
}

func (s *TestService) Start() error {
	r, err := miniredis.Run()
	if err != nil {
		return err
	}
	s.Service = r
	if s.Clock == nil {
		s.Clock = clockwork.NewFakeClock()
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
		conn:  s.pool.Get(),
		Clock: s.Clock,
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
