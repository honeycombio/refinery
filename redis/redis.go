package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/facebookgo/startstop"
	"github.com/gofrs/uuid/v5"
	"github.com/gomodule/redigo/redis"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/metrics"
	"github.com/jonboulle/clockwork"
)

// A ping is set to the server with this period to test for the health of
// the connection and server.
const HealthCheckPeriod = time.Minute

var ErrKeyNotFound = errors.New("key not found")

type Script interface {
	Load(conn Conn) error
	Do(ctx context.Context, conn Conn, keysAndArgs ...any) (any, error)
	DoStrings(ctx context.Context, conn Conn, keysAndArgs ...any) ([]string, error)
	DoInt(ctx context.Context, conn Conn, keysAndArgs ...any) (int, error)
	SendHash(ctx context.Context, conn Conn, keysAndArgs ...any) error
	Send(ctx context.Context, conn Conn, keysAndArgs ...any) error
}

type Client interface {
	Get() Conn
	GetContext(context.Context) (Conn, error)
	NewScript(keyCount int, src string) Script
	ListenPubSubChannels(func() error, func(string, []byte), func(string), <-chan struct{}, ...string) error
	GetPubSubConn() PubSubConn
	startstop.Starter
	startstop.Stopper
	Stats() redis.PoolStats
}

type Conn interface {
	AcquireLock(string, time.Duration) (bool, func() error)
	AcquireLockWithRetries(context.Context, string, time.Duration, int, time.Duration) (bool, func() error)
	Close() error
	Del(...string) command
	Get(string) command
	Expire(string, time.Duration) command
	Increment(string) command

	HGet(string, string) command
	HGetAll(string) command
	HGetAllValues(string) command
	HMSet(string, any) command
	HIncrementBy(string, string, int64) command

	SetNXHash(string, ...any) command
	SAdd(string, ...any) command

	ZScore(string, string) command
	ZRemove(string, []string) command
	ZRandom(string, int) command
	ZCount(string, int64, int64) command

	ReceiveStrings(int) ([]string, error)
	ReceiveStructs(int, func([]any, error) error) error
	ReceiveByteSlices(int) ([][][]byte, error)
	ReceiveInt64s(int) ([]int64, error)
	Do(string, ...any) (any, error)
	Exec(...Command) error
	// Flush flushes all buffered commands to the server.
	Flush() error
	MemoryStats() (map[string]any, error)
}

type PubSubConn interface {
	Publish(channel string, message interface{}) error
	Close() error
}

type DefaultPubSubConn struct {
	conn    redis.PubSubConn
	metrics metrics.Metrics
	clock   clockwork.Clock
}

func (d *DefaultPubSubConn) Publish(channel string, message interface{}) error {
	return d.conn.Conn.Send("PUBLISH", channel, message)
}

func (d *DefaultPubSubConn) Close() error {
	return d.conn.Close()
}

var _ Client = &DefaultClient{}

type DefaultClient struct {
	pool    *redis.Pool
	Config  config.RedisConfig `inject:""`
	Metrics metrics.Metrics    `inject:"genericMetrics"`
	Health  health.Recorder    `inject:""`

	// An overwritable clockwork.Clock for test injection
	Clock clockwork.Clock
}

type DefaultConn struct {
	conn    redis.Conn
	metrics metrics.Metrics

	// An overwritable clockwork.Clock for test injection
	Clock clockwork.Clock
}

type DefaultScript struct {
	script *redis.Script
}

func buildOptions(c config.RedisConfig) []redis.DialOption {
	options := []redis.DialOption{
		redis.DialReadTimeout(HealthCheckPeriod + 10*time.Second),
		redis.DialConnectTimeout(30 * time.Second),
		redis.DialDatabase(c.GetRedisDatabase()),
	}

	username := c.GetRedisUsername()
	if username != "" {
		options = append(options, redis.DialUsername(username))
	}

	password := c.GetRedisPassword()
	if password != "" {
		options = append(options, redis.DialPassword(password))
	}

	useTLS := c.GetUseTLS()
	tlsInsecure := c.GetUseTLSInsecure()
	if useTLS {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if tlsInsecure {
			tlsConfig.InsecureSkipVerify = true
		}

		options = append(options,
			redis.DialTLSConfig(tlsConfig),
			redis.DialUseTLS(true))
	}

	return options
}

func (d *DefaultClient) Start() error {
	redisHost := d.Config.GetRedisHost()

	if redisHost == "" {
		redisHost = "localhost:6379"
	}
	options := buildOptions(d.Config)
	pool := &redis.Pool{
		MaxIdle:     d.Config.GetRedisMaxIdle(),
		MaxActive:   d.Config.GetRedisMaxActive(),
		IdleTimeout: d.Config.GetPeerTimeout(),
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			// if redis is started at the same time as refinery, connecting to redis can
			// fail and cause refinery to error out.
			// Instead, we will try to connect to redis for up to 10 seconds with
			// a 1 second delay between attempts to allow the redis process to init
			var (
				conn redis.Conn
				err  error
			)
			for timeout := time.After(10 * time.Second); ; {
				select {
				case <-timeout:
					return nil, err
				default:
					if authCode := d.Config.GetRedisAuthCode(); authCode != "" {
						conn, err = redis.Dial("tcp", redisHost, options...)
						if err != nil {
							return nil, err
						}
						if _, err := conn.Do("AUTH", authCode); err != nil {
							conn.Close()
							return nil, err
						}
						return conn, nil
					} else {
						conn, err = redis.Dial("tcp", redisHost, options...)
						if err == nil {
							return conn, nil
						}
					}
					time.Sleep(time.Second)
				}
			}
		},
	}

	d.pool = pool
	d.Metrics.Register("redis_request_latency", "histogram")

	return nil
}

func (d *DefaultClient) Stop() error {
	return d.pool.Close()
}

func (d *DefaultClient) Stats() redis.PoolStats {
	return d.pool.Stats()
}

// Get returns a connection from the underlying pool. Return this connection to
// the pool with conn.Close().
func (d *DefaultClient) Get() Conn {
	return &DefaultConn{
		conn:    d.pool.Get(),
		metrics: d.Metrics,
		Clock:   clockwork.NewRealClock(),
	}
}

func (d *DefaultClient) GetContext(ctx context.Context) (Conn, error) {
	conn, err := d.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	return &DefaultConn{
		conn:    conn,
		metrics: d.Metrics,
		Clock:   clockwork.NewRealClock(),
	}, nil
}

func (d *DefaultClient) GetPubSubConn() PubSubConn {
	return &DefaultPubSubConn{
		conn: redis.PubSubConn{Conn: d.pool.Get()},
	}

}

// listenPubSubChannels listens for messages on Redis pubsub channels. The
// onStart function is called after the channels are subscribed. The onMessage
// function is called for each message.
func (d *DefaultClient) ListenPubSubChannels(onStart func() error,
	onMessage func(channel string, data []byte), onHealthCheck func(data string), shutdown <-chan struct{},
	channels ...string) error {
	// Read timeout on server should be greater than ping period.
	c := d.pool.Get()

	psc := redis.PubSubConn{Conn: c}
	defer func() { psc.Close() }()

	if err := psc.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return err
	}

	done := make(chan error, 1)

	// Start a goroutine to receive notifications from the server.
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case error:
				done <- n
				return
			case redis.Pong:
				onHealthCheck(n.Data)
			case redis.Message:
				onMessage(n.Channel, n.Data)
			case redis.Subscription:
				switch n.Count {
				case len(channels):
					// Notify application when all channels are subscribed.
					if onStart == nil {
						continue
					}
					if err := onStart(); err != nil {
						done <- err
						return
					}
				case 0:
					// Return from the goroutine when all channels are unsubscribed.
					done <- nil
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(HealthCheckPeriod)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ticker.C:
			// Send ping to test health of connection and server. If
			// corresponding pong is not received, then receive on the
			// connection will timeout and the receive goroutine will exit.
			if err := psc.Ping(""); err != nil {
				return err
			}
		case <-shutdown:
			break loop
		case err := <-done:
			// Return error from the receive goroutine.
			return err
		}
	}

	// Signal the receiving goroutine to exit by unsubscribing from all channels.
	if err := psc.Unsubscribe(); err != nil {
		return err
	}

	// Wait for goroutine to complete.
	return <-done
}

// NewScript returns a new script object that can be optionally registered with
// the redis server (using Load) and then executed (using Do).
func (c *DefaultClient) NewScript(keyCount int, src string) Script {
	return &DefaultScript{
		script: redis.NewScript(keyCount, src),
	}
}

// AcquireLock attempts to acquire a lock for the given cacheKey
// returns a boolean indicating success, and a function that will unlock the lock.
func (c *DefaultConn) AcquireLock(key string, ttl time.Duration) (bool, func() error) {
	lock := uuid.Must(uuid.NewV4()).String()

	// See more: https://redis.io/topics/distlock#correct-implementation-with-a-single-instance
	// NX -- Only set the key if it does not already exist.
	// PX milliseconds -- Set the specified expire time, in milliseconds.
	s, err := redis.String(c.conn.Do("SET", key, lock, "NX", "PX", ttl.Milliseconds()))

	success := err == nil && s == "OK"
	if success {
		return true, func() error {
			// clear the lock
			script := `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
			res, err := c.conn.Do("EVAL", script, 1, key, lock)
			if err != nil {
				return err
			}
			amountKeysDeleted, ok := res.(int64)
			if !ok {
				return errors.New("unexpected type from redis while clearing lock")
			}
			if amountKeysDeleted == 0 {
				return errors.New("lock not found")
			}
			if amountKeysDeleted > 1 {
				return fmt.Errorf("unexpectedly deleted %d keys from redis while clearing lock for %s", amountKeysDeleted, key)
			}
			return nil
		}
	} else {
		return false, func() error { return nil }
	}
}

// AcquireLockWithRetries will attempt to acquire a lock for the given cacheKey, up to maxRetries times.
// returns a boolean indicating success, and a function that will unlock the lock.
func (c *DefaultConn) AcquireLockWithRetries(ctx context.Context, key string, ttl time.Duration, maxRetries int, retryPause time.Duration) (bool, func() error) {
	for i := 0; i < maxRetries; i++ {

		if success, unlock := c.AcquireLock(key, ttl); success {
			return true, func() error {
				err := unlock()
				return err
			}
		}

		select {
		case <-ctx.Done():
			return false, func() error { return nil }
		case <-c.Clock.After(retryPause):
		}
	}

	return false, func() error { return nil }
}

func (c *DefaultConn) Close() error {
	return c.conn.Close()
}

func (c *DefaultConn) Del(keys ...string) command {
	args := redis.Args{}.AddFlat(keys)
	return command{
		name: "DEL",
		args: args,
		conn: c,
	}
}

func (c *DefaultConn) Exists(key string) command {
	return command{
		name: "EXISTS",
		args: redis.Args{key},
		conn: c,
	}
}

func (c *DefaultConn) Expire(key string, ttl time.Duration) command {
	return command{
		name: "EXPIRE",
		args: redis.Args{key, ttl.Seconds()},
		conn: c,
	}
}

func (c *DefaultConn) Get(key string) command {
	return command{
		name: "GET",
		args: redis.Args{key},
		conn: c,
	}
}

// ZAdd adds a member to a sorted set at key with a score, only if the member does not already exist
func (c *DefaultConn) ZAdd(key string, args []interface{}) command {
	argsList := redis.Args{key, "NX"}.AddFlat(args)
	return command{
		name: "ZADD",
		args: argsList,
		conn: c,
	}
}

func (c *DefaultConn) ZRange(key string, start, stop int) command {
	return command{
		name: "ZRANGE",
		args: redis.Args{key, start, stop},
		conn: c,
	}
}

func (c *DefaultConn) ZScore(key string, member string) command {
	return command{
		name: "ZSCORE",
		args: redis.Args{key, member},
		conn: c,
	}
}

func (c *DefaultConn) ZMScore(key string, members []string) command {
	args := redis.Args{key}.AddFlat(members)
	return command{
		name: "ZMSCORE",
		args: args,
		conn: c,
	}
}

func (c *DefaultConn) ZCard(key string) command {
	return command{
		name: "ZCARD",
		args: redis.Args{key},
		conn: c,
	}
}

func (c *DefaultConn) ZRandom(key string, count int) command {
	return command{
		name: "ZRANDMEMBER",
		args: redis.Args{key, count},
		conn: c,
	}
}

func (c *DefaultConn) ZRemove(key string, members []string) command {
	args := redis.Args{key}.AddFlat(members)
	return command{
		name: "ZREM",
		args: args,
		conn: c,
	}
}

func (c *DefaultConn) SetNXHash(key string, val ...interface{}) command {
	args := redis.Args{key}.AddFlat(val)

	return command{
		name: "HSETNX",
		args: args,
		conn: c,
	}
}

func (c *DefaultConn) ZCount(key string, start int64, stop int64) command {
	startArg := strconv.FormatInt(start, 10)
	stopArg := strconv.FormatInt(stop, 10)
	if start == 0 {
		startArg = "-inf"
	}

	if stop == -1 {
		stopArg = "+inf"
	}

	return command{
		name: "ZCOUNT",
		args: redis.Args{key, startArg, stopArg},
		conn: c,
	}
}

func (c *DefaultConn) Increment(key string) command {
	return command{
		name: "INCR",
		args: redis.Args{key},
		conn: c,
	}
}

func (c *DefaultConn) HIncrementBy(key string, field string, amount int64) command {
	return command{
		name: "HINCRBY",
		args: redis.Args{key, field, amount},
		conn: c,
	}
}

func (c *DefaultConn) HGet(key string, field string) command {
	return command{
		name: "HGET",
		args: redis.Args{key, field},
		conn: c,
	}
}

func (c *DefaultConn) HSet(key string, field string, value interface{}) command {
	return command{
		name: "HSET",
		args: redis.Args{key, field, value},
		conn: c,
	}
}

func (c *DefaultConn) HMSet(key string, values any) command {
	args := redis.Args{key}.AddFlat(values)
	return command{
		name: "HMSET",
		args: args,
		conn: c,
	}
}

func (c *DefaultConn) HGetAll(key string) command {
	return command{
		name: "HGETALL",
		args: redis.Args{key},
		conn: c,
	}
}

func (c *DefaultConn) HGetAllValues(key string) command {
	return command{
		name: "HVALS",
		args: redis.Args{key},
		conn: c,
	}
}

func (c *DefaultConn) SAdd(key string, members ...any) command {
	args := redis.Args{key}.Add(members...)
	return command{
		name: "SADD",
		args: args,
		conn: c,
	}
}

func (c *DefaultConn) Exec(commands ...Command) error {
	err := c.conn.Send("MULTI")
	if err != nil {
		return err
	}

	for _, command := range commands {
		err = c.conn.Send(command.Name(), command.Args()...)
		if err != nil {
			return err
		}
	}

	_, err = redis.Values(c.conn.Do("EXEC"))
	if err != nil {
		return err
	}

	return nil
}

func (c *DefaultConn) Flush() error {
	return c.conn.Flush()
}

// MemoryStats returns the memory statistics reported by the redis server
// for full list of stats see https://redis.io/commands/memory-stats
func (c *DefaultConn) MemoryStats() (map[string]any, error) {
	values, err := redis.Values(c.conn.Do("MEMORY", "STATS"))
	if err != nil {
		return nil, err
	}

	result := make(map[string]any, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].([]byte)
		if !ok {
			return nil, fmt.Errorf("unexpected type from redis while parsing memory stats")
		}
		result[string(key)] = values[i+1]
	}

	return result, nil
}

func (c *DefaultConn) ReceiveInt64s(n int) ([]int64, error) {
	replies := make([]int64, 0, n)
	err := c.receive(n, func(reply any, err error) error {
		if err != nil {
			return err
		}
		val, err := redis.Int64(reply, nil)
		if errors.Is(err, redis.ErrNil) {
			replies = append(replies, -1)
			return nil
		}
		if err != nil {
			return err
		}
		replies = append(replies, val)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return replies, nil
}

func (c *DefaultConn) ReceiveStrings(n int) ([]string, error) {
	replies := make([]string, 0, n)
	err := c.receive(n, func(reply any, err error) error {
		if err != nil {
			return err
		}
		val, err := redis.String(reply, nil)
		if errors.Is(err, redis.ErrNil) {
			replies = append(replies, "")
			return nil
		}
		if err != nil {
			return err
		}
		replies = append(replies, val)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return replies, nil
}

// ReceiveByteSlices receives n replies from a batch of array return values and converts them to
// a slice of byte slices. If a reply is nil, it will be represented as a nil slice.
//
// For example: command `HVALS` returns a slice of byte slices, where each byte slice is a value
// in the hash. An array of return values from a batch of `HVALS` commands will be a slice of byte slices.
func (c *DefaultConn) ReceiveByteSlices(n int) ([][][]byte, error) {
	replies := make([][][]byte, 0, n)
	err := c.receive(n, func(reply any, err error) error {
		if err != nil {
			return err
		}
		val, err := redis.ByteSlices(reply, nil)
		if errors.Is(err, redis.ErrNil) {
			replies = append(replies, nil)
			return nil
		}
		if err != nil {
			return err
		}
		replies = append(replies, val)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return replies, nil
}

func (c *DefaultConn) ReceiveStructs(n int, converter func(reply []any, err error) error) error {
	return c.receive(n, func(reply any, err error) error {
		values, err := redis.Values(reply, err)
		if err != nil {
			return err
		}

		return converter(values, err)
	})
}

func (c *DefaultConn) receive(n int, converter func(reply any, err error) error) error {
	err := c.conn.Flush()
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		err := converter(c.conn.Receive())
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *DefaultConn) Do(commandString string, args ...any) (any, error) {
	return c.conn.Do(commandString, args...)
}

func (s *DefaultScript) Load(conn Conn) error {
	defaultConn := conn.(*DefaultConn)
	return s.script.Load(defaultConn.conn)
}

func (s *DefaultScript) DoStrings(ctx context.Context, conn Conn, keysAndArgs ...any) ([]string, error) {
	defaultConn := conn.(*DefaultConn)
	result, err := s.script.Do(defaultConn.conn, keysAndArgs...)

	if v, err := redis.Int(result, err); err == nil {
		if v == -1 {
			return nil, ErrKeyNotFound
		}

		return nil, fmt.Errorf("unexpected integer response from redis: %d", v)
	}

	return redis.Strings(result, err)
}

func (s *DefaultScript) DoInt(ctx context.Context, conn Conn, keysAndArgs ...any) (int, error) {
	defaultConn := conn.(*DefaultConn)
	result, err := s.script.Do(defaultConn.conn, keysAndArgs...)
	return redis.Int(result, err)
}

func (s *DefaultScript) Do(ctx context.Context, conn Conn, keysAndArgs ...any) (any, error) {
	defaultConn := conn.(*DefaultConn)
	return s.script.DoContext(ctx, defaultConn.conn, keysAndArgs...)
}

func (s *DefaultScript) SendHash(ctx context.Context, conn Conn, keysAndArgs ...any) error {
	defaultConn := conn.(*DefaultConn)
	return s.script.SendHash(defaultConn.conn, keysAndArgs...)
}

func (s *DefaultScript) Send(ctx context.Context, conn Conn, keysAndArgs ...any) error {
	defaultConn := conn.(*DefaultConn)
	return s.script.Send(defaultConn.conn, keysAndArgs...)
}

func (s *DefaultScript) Hash() string {
	return s.script.Hash()
}

var _ Command = command{}

type command struct {
	name string
	args []any
	conn Conn
}

// Do executes the command and returns the result from redis.
func (c command) Do() (any, error) {
	defaultConn := c.conn.(*DefaultConn)

	resp, err := defaultConn.conn.Do(c.Name(), c.Args()...)
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return nil, ErrKeyNotFound
		}
	}

	return resp, nil
}

func (c command) DoStrings() ([]string, error) {
	return redis.Strings(c.Do())
}

func (c command) DoInt64() (int64, error) {
	return redis.Int64(c.Do())
}

// Send buffers the command in the connection's write buffer.
// The command is sent to redis when the connection's Flush method is called.
func (c command) Send() error {
	defaultConn := c.conn.(*DefaultConn)

	return defaultConn.conn.Send(c.Name(), c.Args()...)
}

func (c command) Args() []any {
	return c.args
}

func (c command) Name() string {
	return c.name
}

type Command interface {
	Name() string
	Args() []any
}

// Args is a helper function to convert a list of arguments to a redis.Args
// It returns the result the flattened value of args.
func Args(args ...any) redis.Args {
	return redis.Args{}.AddFlat(args)
}

func ScanStruct(reply []any, out any) error {
	return redis.ScanStruct(reply, out)
}
