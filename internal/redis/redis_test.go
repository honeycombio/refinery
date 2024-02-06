package redis_test

// import (
// 	"context"
// 	"testing"
// 	"time"

// 	"github.com/gofrs/uuid/v5"
// 	"github.com/honeycombio/refinery/internal/redis"
// 	"github.com/jonboulle/clockwork"
// 	"github.com/stretchr/testify/assert"

// 	"github.com/honeycombio/hound/test"
// )

// func TestAcquireLockWithRetriesCancel(t *testing.T) {
// 	t.Parallel()
// 	ctx := context.Background()

// 	h := NewRedisTestHarness(ctx, t)
// 	defer h.Stop(ctx)

// 	cacheKey := createArbitraryUniqueKey()

// 	gotLock, clearLock1 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, time.Minute, 5, time.Second)
// 	assert.Equal(t, gotLock, true)

// 	innerCtx, cancel := context.WithCancel(context.Background())
// 	var clearLock2 func() error
// 	done := make(chan bool, 1)
// 	go func() {
// 		gotLock, clearLock2 = h.Conn.AcquireLockWithRetries(innerCtx, cacheKey, time.Minute, 5, time.Second)
// 		done <- true
// 	}()

// 	cancel()
// 	<-done
// 	assert.Equal(t, gotLock, false) // should not get the lock
// 	assert.NoError(t, clearLock2()) // should noop without errors
// 	assert.NoError(t, clearLock1())

// 	gotLock, clearLock3 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, time.Minute, 5, time.Second)
// 	assert.Equal(t, gotLock, true)
// 	assert.NoError(t, clearLock3())
// }

// func TestAcquireLockWithRetriesTimeout(t *testing.T) {
// 	t.Parallel()
// 	ctx := context.Background()

// 	h := NewRedisTestHarness(ctx, t)
// 	defer h.Stop(ctx)

// 	redis := h.Redis.Client.Get()
// 	defer redis.Close()

// 	cacheKey := createArbitraryUniqueKey()
// 	ttl := time.Minute
// 	retries := 5
// 	retryPause := time.Second

// 	gotLock, clearLock1 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
// 	test.Equals(t, gotLock, true)

// 	var clearLock2 func() error
// 	done := make(chan bool, 1)
// 	go func() {
// 		gotLock, clearLock2 = h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
// 		done <- true
// 	}()

// 	// iterate through all the retries
// 	for i := 0; i < retries; i++ {
// 		h.Clock.BlockUntil(1)
// 		h.Clock.Advance(retryPause)
// 	}

// 	<-done
// 	test.Equals(t, gotLock, false) // should not get the lock
// 	test.OK(t, clearLock2())       // should noop without errors
// 	test.OK(t, clearLock1())

// 	gotLock, clearLock3 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
// 	test.Equals(t, gotLock, true)
// 	test.OK(t, clearLock3())
// }

// func TestAcquireLockWithRetriesTTLExceeded(t *testing.T) {
// 	t.Parallel()
// 	ctx := context.Background()

// 	h := NewRedisTestHarness(ctx, t)
// 	defer h.Stop(ctx)

// 	redis := h.Redis.Client.Get()
// 	defer redis.Close()

// 	cacheKey := createArbitraryUniqueKey()
// 	ttl := time.Minute
// 	retries := 5
// 	retryPause := time.Second

// 	gotLock, clearLock1 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
// 	test.Equals(t, gotLock, true)

// 	var clearLock2 func() error
// 	done := make(chan bool, 1)
// 	go func() {
// 		gotLock, clearLock2 = h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
// 		done <- true
// 	}()

// 	// wait until redis TTL
// 	h.Clock.BlockUntil(1)
// 	h.Clock.Advance(retryPause)
// 	h.Redis.Server.FastForward(ttl)

// 	<-done
// 	test.Equals(t, gotLock, true) // should get the lock
// 	test.OK(t, clearLock2())      // should clear the lock
// 	test.Err(t, clearLock1())     // should error because the lock is gone (due to TTL)

// 	gotLock, clearLock3 := h.Conn.AcquireLockWithRetries(ctx, cacheKey, ttl, retries, retryPause)
// 	test.Equals(t, gotLock, true)
// 	test.OK(t, clearLock3())
// }

// func Test_MGetSetings(t *testing.T) {
// 	ctx := context.Background()

// 	h := NewRedisTestHarness(ctx, t)
// 	defer h.Stop(ctx)

// 	redis := h.Redis.Client.Get()
// 	defer redis.Close()

// 	redis.SetString("foo", "fooval")
// 	redis.SetString("bar", "barval")

// 	vals, err := redis.MGetStrings("foo", "bar", "baz")
// 	test.Nil(t, err)
// 	test.Equals(t, []string{"fooval", "barval", ""}, vals)
// }

// func Test_SetStringTTL(t *testing.T) {
// 	ctx := context.Background()

// 	h := NewRedisTestHarness(ctx, t)
// 	defer h.Stop(ctx)

// 	redis := h.Redis.Client.Get()
// 	defer redis.Close()

// 	ttlDays := 30
// 	ttl := time.Duration(ttlDays*24) * time.Hour

// 	_, err := redis.SetStringTTL(ctx, "foo", "fooval", ttl)
// 	test.Nil(t, err)

// 	val, err := redis.GetString(ctx, "foo")
// 	test.Nil(t, err)
// 	test.Equals(t, "fooval", val)
// }

// func Test_SetStringsTTL(t *testing.T) {
// 	ctx := context.Background()

// 	h := NewRedisTestHarness(ctx, t)
// 	defer h.Stop(ctx)

// 	redis := h.Redis.Client.Get()
// 	defer redis.Close()

// 	ttlDays := 30
// 	ttl := time.Duration(ttlDays*24) * time.Hour

// 	_, err := redis.SetStringsTTL([]string{"foo", "bar"}, []string{"fooval", "barval"}, ttl)
// 	test.Nil(t, err)

// 	vals, err := redis.GetStrings("foo", "bar", "baz")
// 	test.Nil(t, err)
// 	test.Equals(t, []string{"fooval", "barval", ""}, vals)
// }

// type CachingClientHarness struct {
// 	Clock clockwork.FakeClock
// 	Redis hnytest.TestRedis
// 	Conn  redis.Conn
// }

// func NewRedisTestHarness(ctx context.Context, t *testing.T) *CachingClientHarness {
// 	h := &CachingClientHarness{}

// 	h.Redis = hnytest.TestRedis{}
// 	h.Redis.Start(ctx)

// 	conn := h.Redis.Client.Get().(*hnytest.RedisConnStub)
// 	h.Clock = conn.DefaultConn.Clock.(clockwork.FakeClock)
// 	h.Conn = conn

// 	return h
// }

// func (h *CachingClientHarness) Stop(ctx context.Context) {
// 	h.Conn.Close()
// 	h.Redis.Stop(context.Background())
// }

// func createArbitraryUniqueKey() string {
// 	return uuid.Must(uuid.NewV4()).String()
// }
