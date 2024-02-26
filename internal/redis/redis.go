package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/gomodule/redigo/redis"
	"github.com/jonboulle/clockwork"
)

type Config struct {
	Addr           string        `cfg:"addr"`
	IdleTimeout    time.Duration `cfg:"idle_timeout"`
	ConnectTimeout time.Duration `cfg:"connect_timeout"`
	ReadTimeout    time.Duration `cfg:"read_timeout"`
	MaxIdle        int           `cfg:"max_idle"`
	MaxActive      int           `cfg:"max_active"`
	Database       int           `cfg:"database"`
}

type Script interface {
	Load(conn Conn) error
	Do(ctx context.Context, conn Conn, keysAndArgs ...any) (any, error)
	SendHash(ctx context.Context, conn Conn, keysAndArgs ...any) error
	Send(ctx context.Context, conn Conn, keysAndArgs ...any) error
}

type Client interface {
	Get() Conn
	NewScript(keyCount int, src string) Script
	Stop(context.Context) error
}

type Conn interface {
	AcquireLock(string, time.Duration) (bool, func() error)
	AcquireLockWithRetries(context.Context, string, time.Duration, int, time.Duration) (bool, func() error)
	Close() error
	Del(...string) (int64, error)
	Exists(string) (bool, error)
	Expire(string, time.Duration) error
	GetInt64(string) (int64, error)
	GetInt64NoDefault(string) (int64, error)
	GetString(context.Context, string) (string, error)
	GetStrings(...string) ([]string, error)
	MGetStrings(...string) ([]string, error)
	IncrementAndExpire(string, time.Duration) error
	IncrementBy(string, int64) (int64, error)
	ListKeys(string) ([]string, error)
	Scan(string, string, <-chan struct{}) (<-chan string, <-chan error)
	SetIfNotExistsTTLInt64(string, int64, int) error
	SetIfNotExistsTTLString(string, string, int) (any, error)
	SetInt64(string, int64) error
	SetInt64TTL(string, int64, int) error
	SetString(string, string) (string, error)
	SetStringsTTL([]string, []string, time.Duration) ([]any, error)
	SetStringTTL(context.Context, string, string, time.Duration) (string, error)

	GetStringHash(string) (map[string]string, error)
	GetStructHash(string, any) error
	GetSliceOfStructsHash(string, any) error
	GetFloat64Hash(string) (map[string]float64, error)
	ListFields(string) ([]string, error)
	IncrementByHash(string, string, int64) (int64, error)
	SetHash(string, any) error
	SetHashTTL(string, any, time.Duration) (any, error)

	SAdd(string, ...any) error

	RPush(string, any) error
	RPushTTL(string, string, time.Duration) (bool, error)
	LRange(string, int, int) ([]any, error)
	LIndexString(string, int) (string, error)

	ZAdd(string, []any) error
	ZMove(string, string, []int64, []any) error
	ZRange(string, int, int) ([]string, error)
	ZRangeByScoreString(string, int64) ([]string, error)
	ZScore(string, string) (int64, error)
	ZMScore(string, []string) ([]int64, error)
	ZCard(string) (int64, error)
	ZExist(string, string) (bool, error)
	ZRemove(string, []string) error
	TTL(string) (int64, error)

	Do(string, ...any) (any, error)
	Exec(...Command) error
	Watch(string, func(Conn) error) (func() error, error)
}

type DefaultClient struct {
	pool   *redis.Pool
	Config *Config
}

type DefaultConn struct {
	conn redis.Conn

	// An overwritable clockwork.Clock for test injection
	Clock clockwork.Clock
}

type DefaultScript struct {
	script *redis.Script
}

func NewClient(cfg *Config) Client {
	pool := &redis.Pool{
		MaxIdle:     cfg.MaxIdle,
		MaxActive:   cfg.MaxActive,
		IdleTimeout: cfg.IdleTimeout,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(
				"tcp", cfg.Addr,
				redis.DialReadTimeout(cfg.ReadTimeout),
				redis.DialConnectTimeout(cfg.ConnectTimeout),
				redis.DialDatabase(cfg.Database),
			)
			return c, err
		},
	}

	return &DefaultClient{
		pool:   pool,
		Config: cfg,
	}
}

func (d *DefaultClient) Stop(ctx context.Context) error {
	return d.pool.Close()
}

// Get returns a connection from the underlying pool. Return this connection to
// the pool with conn.Close().
func (d *DefaultClient) Get() Conn {
	return &DefaultConn{
		conn:  d.pool.Get(),
		Clock: clockwork.NewRealClock(),
	}
}

// NewScript returns a new script object that can be optionally registered with
// the redis server (using Load) and then executed (using Do).
func (c *DefaultClient) NewScript(keyCount int, src string) Script {
	return &DefaultScript{
		script: redis.NewScript(keyCount, src),
	}
}

func (c *DefaultConn) Close() error {
	return c.conn.Close()
}

func (c *DefaultConn) Del(keys ...string) (int64, error) {
	args := redis.Args{}.AddFlat(keys)
	return redis.Int64(c.conn.Do("DEL", args...))
}

func (c *DefaultConn) Exists(key string) (bool, error) {
	return redis.Bool(c.conn.Do("EXISTS", key))
}

func (c *DefaultConn) GetInt64(key string) (int64, error) {
	v, err := c.GetInt64NoDefault(key)
	if err == redis.ErrNil {
		return 0, nil
	}
	return v, err
}

func (c *DefaultConn) GetInt64NoDefault(key string) (int64, error) {
	return redis.Int64(c.conn.Do("GET", key))
}

func (c *DefaultConn) SetString(key, val string) (string, error) {
	return redis.String(c.conn.Do("SET", key, val))
}

func (c *DefaultConn) SetStringTTL(ctx context.Context, key, val string, ttl time.Duration) (string, error) {
	val, err := redis.String(c.conn.Do("SET", key, val, "EX", int(ttl/time.Second)))
	return val, err
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

func (c *DefaultConn) SetStringsTTL(keys, vals []string, ttl time.Duration) ([]any, error) {
	if err := c.conn.Send("MULTI"); err != nil {
		return nil, err
	}
	for i := range keys {
		if err := c.conn.Send("SET", keys[i], vals[i], "EX", int(ttl/time.Second)); err != nil {
			return nil, err
		}
	}
	// TODO: values is always "OK", but we should be able to get the values
	// for the items in the batch
	values, err := redis.Values(c.conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (c *DefaultConn) GetString(ctx context.Context, key string) (string, error) {

	v, err := redis.String(c.conn.Do("GET", key))
	if err == redis.ErrNil {
		return "", nil
	}
	return v, err
}

func (c *DefaultConn) GetStrings(keys ...string) ([]string, error) {
	if err := c.conn.Send("MULTI"); err != nil {
		return nil, err
	}
	for _, key := range keys {
		if err := c.conn.Send("GET", key); err != nil {
			return nil, err
		}
	}
	values, err := redis.Values(c.conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}
	r := make([]string, 0)
	if err = redis.ScanSlice(values, &r); err != nil {
		return nil, err
	}
	return r, nil
}

func (c *DefaultConn) MGetStrings(keys ...string) ([]string, error) {
	args := make([]any, len(keys))
	for i, k := range keys {
		args[i] = k
	}

	values, err := redis.Strings(c.conn.Do("MGET", args...))
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (c *DefaultConn) SetIfNotExistsTTLString(key string, val string, ttlSeconds int) (any, error) {
	return c.conn.Do("SET", key, val, "EX", ttlSeconds, "NX")
}

func (c *DefaultConn) IncrementBy(key string, incrVal int64) (int64, error) {
	return redis.Int64(c.conn.Do("INCRBY", key, incrVal))
}

func (c *DefaultConn) SetInt64(key string, val int64) error {
	_, err := c.conn.Do("SET", key, val)
	return err
}

func (c *DefaultConn) SetInt64TTL(key string, val int64, ttl int) error {
	_, err := c.conn.Do("SET", key, val, "EX", ttl)
	return err
}

func (c *DefaultConn) IncrementAndExpire(key string, ttl time.Duration) error {
	if err := c.conn.Send("MULTI"); err != nil {
		return err
	}
	if err := c.conn.Send("INCR", key); err != nil {
		return err
	}
	if err := c.conn.Send("EXPIRE", key, int(ttl/time.Second)); err != nil {
		return err
	}
	_, err := c.conn.Do("EXEC")
	return err
}

func (c *DefaultConn) SetIfNotExistsTTLInt64(key string, val int64, ttlSeconds int) error {
	if err := c.conn.Send("MULTI"); err != nil {
		return err
	}
	if err := c.conn.Send("SETNX", key, val); err != nil {
		return err
	}
	if err := c.conn.Send("EXPIRE", key, ttlSeconds); err != nil {
		return err
	}
	_, err := c.conn.Do("EXEC")
	return err
}

func (c *DefaultConn) ListKeys(prefix string) ([]string, error) {
	return redis.Strings(c.conn.Do("KEYS", prefix))
}

func (c *DefaultConn) Expire(key string, ttl time.Duration) error {
	_, err := c.conn.Do("EXPIRE", key, ttl.Seconds())
	return err
}

func (c *DefaultConn) GetTTL(key string) (int64, error) {
	return redis.Int64(c.conn.Do("TTL", key))
}

func (c *DefaultConn) Scan(pattern, count string, cancel <-chan struct{}) (<-chan string, <-chan error) {
	keyChan := make(chan string)
	errChan := make(chan error)

	go func() {
		cursor := "0"
	Loop:
		for {
			select {
			case <-cancel:
				break Loop
			default:
			}

			values, err := redis.Values(c.conn.Do("SCAN", cursor, "MATCH", pattern, "COUNT", count))
			if err != nil {
				errChan <- err
				break
			}
			if len(values) != 2 {
				errChan <- errors.New("unexpected response format from redis")
				break
			}

			cursor, err = redis.String(values[0], nil)
			if err != nil {
				select {
				case errChan <- err:
					// we wrote to the channel, break
					break Loop
				case <-cancel:
					break Loop
				}
			}

			keys, err := redis.Strings(values[1], nil)
			if err != nil {
				select {
				case errChan <- err:
					// we wrote to the channel, break
					break Loop
				case <-cancel:

					break Loop
				}
			}

			for _, key := range keys {
				select {
				case keyChan <- key:
					// we wrote to the channel, keep looping
				case <-cancel:
					break Loop
				}
			}

			// redis will return 0 when we have iterated over the entire set
			if cursor == "0" {
				break
			}
		}

		close(errChan)
		close(keyChan)
	}()

	return keyChan, errChan
}

func (c *DefaultConn) RPush(key string, val any) error {
	_, err := c.conn.Do("RPUSH", key, val)
	return err
}

func (c *DefaultConn) LRange(key string, start int, end int) ([]any, error) {
	return redis.Values(c.conn.Do("LRANGE", key, start, end))
}

func (c *DefaultConn) LIndexString(key string, index int) (string, error) {
	result, err := redis.String(c.conn.Do("LINDEX", key, index))
	if err == redis.ErrNil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return result, nil
}

// ZAdd adds a member to a sorted set at key with a score, only if the member does not already exist
func (c *DefaultConn) ZAdd(key string, args []interface{}) error {
	argsList := redis.Args{key, "NX"}.AddFlat(args)
	_, err := c.conn.Do("ZADD", argsList...)
	if err == redis.ErrNil {
		return nil
	}
	return err
}

func (c *DefaultConn) ZRange(key string, start, stop int) ([]string, error) {
	return redis.Strings(c.conn.Do("ZRANGE", key, start, stop))
}

func (c *DefaultConn) ZRangeByScoreString(key string, stop int64) ([]string, error) {
	return redis.Strings(c.conn.Do("ZRANGE", key, 0, stop, "BYSCORE"))
}

func (c *DefaultConn) ZScore(key string, member string) (int64, error) {
	return redis.Int64(c.conn.Do("ZSCORE", key, member))
}

func (c *DefaultConn) ZMScore(key string, members []string) ([]int64, error) {
	args := redis.Args{key}.AddFlat(members)
	return redis.Int64s(c.conn.Do("ZMSCORE", args...))
}

func (c *DefaultConn) ZCard(key string) (int64, error) {
	return redis.Int64(c.conn.Do("ZCARD", key))
}

func (c *DefaultConn) ZExist(key string, member string) (bool, error) {
	value, err := redis.Int64(c.conn.Do("ZSCORE", key, member))
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

func (c *DefaultConn) ZMove(fromKey string, toKey string, scores []int64, members []any) error {
	if err := c.conn.Send("MULTI"); err != nil {
		return err
	}

	entries := make([]any, len(members)*2)
	for i := range entries {
		if i%2 == 0 {
			entries[i] = scores[i/2]
		} else {
			entries[i] = members[i/2]
		}

	}
	argsList := redis.Args{toKey, "NX"}.AddFlat(entries)
	if err := c.conn.Send("ZADD", argsList...); err != nil && err != redis.ErrNil {
		return err
	}

	argsList = redis.Args{fromKey}.AddFlat(members)
	if err := c.conn.Send("ZREM", argsList...); err != nil {
		return err
	}
	replies, err := redis.Int64s(c.conn.Do("EXEC"))
	if err != nil {
		return err
	}

	if len(replies) != 2 {
		return errors.New("unexpected response format from redis")
	}

	if replies[0] == 0 {
		err = fmt.Errorf("failed to add member to set %s", toKey)
	}

	return err
}

func (c *DefaultConn) ZRemove(key string, members []string) error {
	args := redis.Args{key}.AddFlat(members)
	_, err := c.conn.Do("ZREM", args...)
	return err
}

func (c *DefaultConn) TTL(key string) (int64, error) {
	return redis.Int64(c.conn.Do("TTL", key))
}

func (c *DefaultConn) GetStringHash(key string) (map[string]string, error) {
	return redis.StringMap(c.conn.Do("HGETALL", key))
}

func (c *DefaultConn) GetFloat64Hash(key string) (map[string]float64, error) {
	return redis.Float64Map(c.conn.Do("HGETALL", key))
}

func (c *DefaultConn) GetStructHash(key string, val interface{}) error {
	values, err := redis.Values(c.conn.Do("HGETALL", key))
	if err != nil {
		return err
	}

	if len(values) == 0 {
		return redis.ErrNil
	}

	return redis.ScanStruct(values, val)
}

func (c *DefaultConn) GetSliceOfStructsHash(key string, val interface{}) error {
	values, err := redis.Values(c.conn.Do("HGETALL", key))
	if err != nil {
		return err
	}
	return redis.ScanSlice(values, val)
}

func (c *DefaultConn) ListFields(key string) ([]string, error) {
	return redis.Strings(c.conn.Do("HKEYS", key))
}

func (c *DefaultConn) SetHash(key string, val interface{}) error {
	args := redis.Args{key}.AddFlat(val)
	_, err := c.conn.Do("HSET", args...)
	return err
}

func (c *DefaultConn) SetHashTTL(key string, val interface{}, expiration time.Duration) (any, error) {
	if err := c.conn.Send("MULTI"); err != nil {
		return nil, err
	}
	args := redis.Args{key}.AddFlat(val)
	err := c.conn.Send("HSET", args...)
	if err != nil {
		return nil, err
	}

	err = c.conn.Send("EXPIRE", key, expiration.Seconds())
	if err != nil {
		return nil, err
	}
	// TODO: values is always "OK", but we should be able to get the values
	// for the items in the batch
	values, err := redis.Values(c.conn.Do("EXEC"))
	if err != nil {
		return nil, err
	}

	return values, nil
}

// returns the value after the increment
func (c *DefaultConn) IncrementByHash(key, field string, incrVal int64) (int64, error) {
	return redis.Int64(c.conn.Do("HINCRBY", key, field, incrVal))
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

func (c *DefaultConn) Do(commandString string, args ...any) (any, error) {
	return c.conn.Do(commandString, args...)
}

func (s *DefaultScript) Load(conn Conn) error {
	defaultConn := conn.(*DefaultConn)
	return s.script.Load(defaultConn.conn)
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

var _ Command = command{}

type command struct {
	name string
	args []any
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

func NewSetHashCommand(key string, value interface{}) command {
	args := redis.Args{key}.AddFlat(value)
	return command{
		name: "HSET",
		args: args,
	}
}

func NewIncrByHashCommand(key, field string, incrVal int64) command {
	return command{
		name: "HINCRBY",
		args: redis.Args{key, field, incrVal},
	}
}

func (c *DefaultConn) RPushTTL(key string, member string, expiration time.Duration) (bool, error) {
	if err := c.conn.Send("MULTI"); err != nil {
		return false, err
	}

	err := c.conn.Send("RPUSH", key, member)
	if err != nil {
		return false, err
	}

	err = c.conn.Send("EXPIRE", key, expiration.Seconds())
	if err != nil {
		return false, err
	}
	// TODO: values is always "OK", but we should be able to get the values
	// for the items in the batch
	results, err := redis.Int64s(c.conn.Do("EXEC"))
	if err != nil {
		return false, err
	}

	if len(results) != 2 {
		return false, errors.New("unexpected response format from redis")
	}

	if results[0] == 0 {
		return false, errors.New("failed to add member to set")
	}

	// TODO: do we care if the ttl is not set?

	return true, nil
}

func (c *DefaultConn) SAdd(key string, members ...any) error {
	args := redis.Args{key}.Add(members...)
	_, err := c.conn.Do("SADD", args...)
	if err != nil {
		return err
	}
	return nil
}

func (c *DefaultConn) Watch(key string, f func(Conn) error) (func() error, error) {
	_, err := c.conn.Do("WATCH", key)
	if err != nil {
		return nil, err
	}

	err = f(c)
	if err != nil {
		return func() error {
			_, err := c.conn.Do("UNWATCH")
			return err
		}, err
	}

	return func() error {
		_, err := c.conn.Do("UNWATCH")
		return err
	}, nil

}
