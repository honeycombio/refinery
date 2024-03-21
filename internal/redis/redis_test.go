package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gofrs/uuid/v5"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/redis"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireLockWithRetriesCancel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	h := NewRedisTestHarness(ctx, t)
	defer h.Stop(ctx)

	cacheKey := createArbitraryUniqueKey()

	gotLock, clearLock1 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, time.Minute, 5, time.Second)
	assert.Equal(t, gotLock, true)

	innerCtx, cancel := context.WithCancel(context.Background())
	var clearLock2 func() error
	done := make(chan bool, 1)
	go func() {
		gotLock, clearLock2 = h.Conn.AcquireLockWithRetries(innerCtx, cacheKey, time.Minute, 5, time.Second)
		done <- true
	}()

	cancel()
	<-done
	assert.Equal(t, gotLock, false) // should not get the lock
	assert.NoError(t, clearLock2()) // should noop without errors
	assert.NoError(t, clearLock1())

	gotLock, clearLock3 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, time.Minute, 5, time.Second)
	assert.Equal(t, gotLock, true)
	assert.NoError(t, clearLock3())
}

func TestAcquireLockWithRetriesTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	h := NewRedisTestHarness(ctx, t)
	defer h.Stop(ctx)

	redis := h.Redis.Client.Get()
	defer redis.Close()

	cacheKey := createArbitraryUniqueKey()
	ttl := time.Minute
	retries := 5
	retryPause := time.Second

	gotLock, clearLock1 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
	require.Equal(t, gotLock, true)

	var clearLock2 func() error
	done := make(chan bool, 1)
	go func() {
		gotLock, clearLock2 = h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
		done <- true
	}()

	// iterate through all the retries
	for i := 0; i < retries; i++ {
		h.Clock.BlockUntil(1)
		h.Clock.Advance(retryPause)
	}

	<-done
	require.False(t, gotLock)        // should not get the lock
	require.NoError(t, clearLock2()) // should noop without errors
	require.NoError(t, clearLock1())

	gotLock, clearLock3 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
	require.True(t, gotLock)
	require.NoError(t, clearLock3())
}

func TestAcquireLockWithRetriesTTLExceeded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	h := NewRedisTestHarness(ctx, t)
	defer h.Stop(ctx)

	redis := h.Redis.Client.Get()
	defer redis.Close()

	cacheKey := createArbitraryUniqueKey()
	ttl := time.Minute
	retries := 5
	retryPause := time.Second

	gotLock, clearLock1 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
	require.True(t, gotLock)

	var clearLock2 func() error
	done := make(chan bool, 1)
	go func() {
		gotLock, clearLock2 = h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
		done <- true
	}()

	// wait until redis TTL
	h.Clock.BlockUntil(1)
	h.Clock.Advance(retryPause)
	h.Redis.Server.FastForward(ttl)

	<-done
	require.True(t, gotLock)        // should get the lock
	assert.NoError(t, clearLock2()) // should clear the lock
	assert.Error(t, clearLock1())   // should error because the lock is gone (due to TTL)

	gotLock, clearLock3 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
	require.True(t, gotLock)
	require.NoError(t, clearLock3())
}

func Test_MGetSetings(t *testing.T) {
	ctx := context.Background()

	h := NewRedisTestHarness(ctx, t)
	defer h.Stop(ctx)

	redis := h.Redis.Client.Get()
	defer redis.Close()

	redis.SetString("foo", "fooval")
	redis.SetString("bar", "barval")

	vals, err := redis.MGetStrings("foo", "bar", "baz")
	require.NoError(t, err)
	require.EqualValues(t, []string{"fooval", "barval", ""}, vals)
}

func Test_SetStringTTL(t *testing.T) {
	ctx := context.Background()

	h := NewRedisTestHarness(ctx, t)
	defer h.Stop(ctx)

	redis := h.Redis.Client.Get()
	defer redis.Close()

	ttlDays := 30
	ttl := time.Duration(ttlDays*24) * time.Hour

	_, err := redis.SetStringTTL(ctx, "foo", "fooval", ttl)
	require.NoError(t, err)

	val, err := redis.GetString(ctx, "foo")
	require.NoError(t, err)
	require.Equal(t, "fooval", val)
}

func Test_SetStringsTTL(t *testing.T) {
	ctx := context.Background()

	h := NewRedisTestHarness(ctx, t)
	defer h.Stop(ctx)

	redis := h.Redis.Client.Get()
	defer redis.Close()

	ttlDays := 30
	ttl := time.Duration(ttlDays*24) * time.Hour

	_, err := redis.SetStringsTTL([]string{"foo", "bar"}, []string{"fooval", "barval"}, ttl)
	require.NoError(t, err)

	vals, err := redis.GetStrings("foo", "bar", "baz")
	require.NoError(t, err)
	require.EqualValues(t, []string{"fooval", "barval", ""}, vals)
}

func createArbitraryUniqueKey() string {
	return uuid.Must(uuid.NewV4()).String()
}

type CachingClientHarness struct {
	Clock clockwork.FakeClock
	Redis TestRedis
	Conn  redis.Conn
}

func NewRedisTestHarness(ctx context.Context, t *testing.T) *CachingClientHarness {
	h := &CachingClientHarness{}

	h.Redis = TestRedis{}
	h.Redis.Start(ctx)

	conn := h.Redis.Client.Get().(*RedisConnStub)
	h.Clock = conn.DefaultConn.Clock.(clockwork.FakeClock)
	h.Conn = conn

	return h
}

func (h *CachingClientHarness) Stop(ctx context.Context) {
	h.Conn.Close()
	h.Redis.Stop(context.Background())
}

type TestRedis struct {
	Server                     *miniredis.Miniredis
	Client                     redis.Client
	MockAcquireLockWithRetries func(context.Context, string, time.Duration, int, time.Duration) (bool, func() error)
}

func (tr *TestRedis) Start(ctx context.Context) {
	tr.Server, _ = miniredis.Run()
	cfg := &config.MockConfig{
		GetRedisHostVal:      tr.Server.Addr(),
		GetRedisDatabaseVal:  0,
		GetRedisMaxActiveVal: 10,
		GetRedisMaxIdleVal:   10,
		GetRedisTimeoutVal:   5,
	}

	client := redis.DefaultClient{Config: cfg, Metrics: &metrics.NullMetrics{}}
	client.Start()

	tr.Client = &RedisClientStub{
		Client:    &client,
		testRedis: tr,
	}
}

func (tr *TestRedis) Stop(ctx context.Context) error {
	err := tr.Client.Stop()
	tr.Server.Close()
	return err
}

type RedisClientStub struct {
	redis.Client
	testRedis *TestRedis
}

func (s *RedisClientStub) Get() redis.Conn {
	conn := s.Client.Get().(*redis.DefaultConn)
	conn.Clock = clockwork.NewFakeClock()
	return &RedisConnStub{
		DefaultConn: *conn,
		testRedis:   s.testRedis,
	}
}

type RedisConnStub struct {
	redis.DefaultConn
	testRedis *TestRedis
}

func (s *RedisConnStub) AcquireLockWithRetries(ctx context.Context, key string, ttl time.Duration, maxRetries int, retryInterval time.Duration) (bool, func() error) {
	if s.testRedis.MockAcquireLockWithRetries != nil {
		return s.testRedis.MockAcquireLockWithRetries(ctx, key, ttl, maxRetries, retryInterval)
	}
	return s.DefaultConn.AcquireLockWithRetries(ctx, key, ttl, maxRetries, retryInterval)
}

func BadRedis() redis.Client {
	return &redis.DefaultClient{Config: &config.MockConfig{GetRedisHostVal: "localhost:0"}}
}
