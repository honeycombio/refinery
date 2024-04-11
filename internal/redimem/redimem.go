package redimem

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/honeycombio/refinery/internal/redis"
	"github.com/sirupsen/logrus"
)

// Membership allows services to register themselves as members of a group and
// the list of all members can be retrieved by any member in the group.
type Membership interface {
	// Register adds name to the list of available servers. The registration lasts
	// for timeout duration. Register should be called again before timeout expires
	// in order to remain a member of the group.
	Register(ctx context.Context, memberName string, timeout time.Duration) error

	// Unregister removes a name from the list immediately. It's intended to be
	// used during shutdown so that there's no delay in the case of deliberate downsizing.
	Unregister(ctx context.Context, memberName string) error

	// GetMembers retrieves the list of all currently registered members. Members
	// that have registered but timed out will not be returned.
	GetMembers(ctx context.Context) ([]string, error)
}

const (
	globalPrefix       = "refinery"
	defaultRepeatCount = 2

	// redisScanTimeout indicates how long to attempt to scan for peers.
	redisScanTimeout = 5 * time.Second

	// redisScanBatchSize indicates how many keys to retrieve from Redis at a time.
	redisScanBatchSize = "1000"
)

// RedisMembership implements the Membership interface using Redis as the backend
type RedisMembership struct {
	// Prefix is a way of namespacing your group membership
	Prefix string
	// Pool should be an already-initialized redis pool
	Pool redis.Client
	// RepeatCount is the number of times GetMembers should ask Redis for the list
	// of members in the pool. As seen in the Redis docs "The SCAN family of
	// commands only offer limited guarantees about the returned elements"
	// (https://redis.io/commands/scan). In order to overcome some of these
	// limitations and gain confidence that the list of members is complete (and
	// preferring to get older members than miss current members), you can
	// configure RedisMembership to repeat the request for all members and union
	// the results, returning a potentially more complete list. This option
	// determines how many times RedisMembership asks Redis for the list of members
	// in the pool before taking the union of results and returning. Defaults to
	// 2.
	RepeatCount int
}

func (rm *RedisMembership) validateDefaults() error {
	if rm.RepeatCount == 0 {
		rm.RepeatCount = defaultRepeatCount
	}
	if rm.Pool == nil {
		// TODO put a mute pool in here or something that will safely noop and not panic
		return errors.New("can't use RedisMembership with an unitialized Redis pool")
	}
	return nil
}

func (rm *RedisMembership) Register(ctx context.Context, memberName string, timeout time.Duration) error {
	err := rm.validateDefaults()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s•%s•%s", globalPrefix, rm.Prefix, memberName)
	timeoutSec := int64(timeout) / int64(time.Second)
	conn, err := rm.Pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("SET", key, "present", "EX", timeoutSec)
	if err != nil {
		logrus.WithField("name", memberName).
			WithField("timeoutSec", timeoutSec).
			WithField("err", err).
			Error("registration failed")
		return err
	}
	return nil
}

func (rm *RedisMembership) Unregister(ctx context.Context, memberName string) error {
	err := rm.validateDefaults()
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s•%s•%s", globalPrefix, rm.Prefix, memberName)
	conn, err := rm.Pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("DEL", key)
	if err != nil {
		logrus.WithField("name", memberName).
			WithField("err", err).
			Error("unregistration failed")
		return err
	}
	return nil
}

// GetMembers reaches out to Redis to retrieve a list of all members in the
// cluster. It does this multiple times (how many is configured on
// initialization) and takes the union of the results returned.
func (rm *RedisMembership) GetMembers(ctx context.Context) ([]string, error) {
	err := rm.validateDefaults()
	if err != nil {
		return nil, err
	}
	// get the list of members multiple times
	allMembers := make([]string, 0)
	for i := 0; i < rm.RepeatCount; i++ {
		mems, err := rm.getMembersOnce(ctx)
		if err != nil {
			return nil, err
		}
		allMembers = append(allMembers, mems...)
	}

	// then sort and compact the list so we get the union of all the previous
	// member requests
	sort.Strings(allMembers)
	members := make([]string, 0, len(allMembers)/rm.RepeatCount)
	var prevMember string
	for _, member := range allMembers {
		if member == prevMember {
			continue
		}
		members = append(members, member)
		prevMember = member
	}
	return members, nil
}

func (rm *RedisMembership) getMembersOnce(ctx context.Context) ([]string, error) {
	keyPrefix := fmt.Sprintf("%s•%s•*", globalPrefix, rm.Prefix)
	conn, err := rm.Pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	keysChan, errChan := rm.scan(conn, keyPrefix, redisScanBatchSize, redisScanTimeout)
	memberList := make([]string, 0)
	for key := range keysChan {
		name := strings.Split(key, "•")[2]
		memberList = append(memberList, name)
	}
	for err := range errChan {
		logrus.WithField("keys_returned", len(memberList)).
			WithField("timeoutSec", redisScanTimeout).
			WithField("err", err).
			Error("redis scan encountered an error")
	}
	return memberList, nil
}

// scan returns two channels and handles all the iteration necessary to get all
// keys from Redis when using the Scan verb by abstracting away the iterator.
// even though scan won't block the redis host, it can still take a long time
// when there are many keys in the redis DB. If the timeout duration elapses,
// scanning will stop and the function will return a timeout value to the error
// channel. There may have been valid results already returned to the keys
// channel, and there may or may not be additional keys in the DB.
func (rm *RedisMembership) scan(conn redis.Conn, pattern, count string, timeout time.Duration) (<-chan string, <-chan error) {
	// make both channels buffered so they can be read in any order instead of both
	done := make(chan struct{})
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go func() {
		<-timer.C
		close(done)
	}()

	return conn.Scan(pattern, count, done)
}
