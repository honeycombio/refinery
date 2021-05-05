package main

// this test is an exercise against an actual redis instance to see the redimem
// package work as expected.

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/refinery/internal/redimem"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	// tick := time.NewTicker(time.Second)
	// for t := range tick.C {
	// 	logrus.Info("Current time: ", t)
	// }

	logrus.SetLevel(logrus.WarnLevel)

	pool := &redis.Pool{
		MaxIdle:     3,
		MaxActive:   30,
		IdleTimeout: 5 * time.Minute,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial(
				"tcp", "localhost:6379",
				redis.DialReadTimeout(1*time.Second),
				redis.DialConnectTimeout(1*time.Second),
				redis.DialDatabase(0), // TODO enable multiple databases for multiple samproxies
			)
		},
	}

	rm := &redimem.RedisMembership{
		Prefix: "test_redimem",
		Pool:   pool,
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			singleTestRandomLength(10, 5, rm)
			wg.Done()
		}()
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
	wg.Wait()
}

// singleTestRandomLength will register then re-register an entry some number of
// times up to limit. It will check the entire time to verify that the entry is
// still there then that it goes away when it's supposed to at the end. The
// intent is to call this function in multiple goroutines to watch a whole slew
// of entries start and stop.
func singleTestRandomLength(limit, registerDurLimitSec int, rm *redimem.RedisMembership) {
	// numTimes will be the number of times to re-register this entry before
	// letting it expire
	numTimes := rand.Intn(limit) + 1
	// registerDur will be the duration in milliseconds to register this entry
	registerDur := rand.Intn(registerDurLimitSec*1000) + 1000
	// reregisterFreq is how frequently we should re-register the entry
	reregisterFreq := registerDur / 2
	// done will let this function know when the entry is done being reregistered
	done := make(chan struct{})
	// name is a random string used to register this entry
	name := GenID(12)

	ctx := context.Background()

	// register the entry once to make sure it's there before the first check runs
	logrus.WithFields(logrus.Fields{
		"registerDur": registerDur,
		"name":        name,
		"numTimes":    numTimes,
	}).Info("registering entry")
	rm.Register(ctx, name, time.Duration(registerDur)*time.Millisecond)

	// register the entry and then re-register it numTimes
	go func() {
		ticker := time.NewTicker(time.Duration(reregisterFreq) * time.Millisecond)
		var i int
		for range ticker.C {
			i = i + 1
			logrus.WithField("name", name).Debug("re-registering entry")
			rm.Register(ctx, name, time.Duration(registerDur)*time.Millisecond)
			if i >= numTimes {
				break
			}
		}
		done <- struct{}{}
	}()

	// watch for the entry to appear, then check that it's still there until it's
	// time for it to go away, then verify it went away.
	func() {
		var i int
	SHOULDEXIST:
		for {
			i = i + 1
			// exit out of this for loop when we get a message from the done channel
			select {
			case <-done:
				break SHOULDEXIST
			default:
			}
			// check that name is registered
			var found bool
			list, err := rm.GetMembers(ctx)
			if err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"name":       name,
					"numEntries": len(list),
					"iteration":  i,
				}).Warn("caught error from get members")
			}
			for _, entry := range list {
				if entry == name {
					found = true
					// logrus.WithField("name", name).Info("shouldexist: found entry")
					break
				}
			}
			if !found {
				logrus.WithFields(logrus.Fields{
					"name":       name,
					"numEntries": len(list),
					"iteration":  i,
				}).Warn("shouldexist: Failed to find entry")
			}
			// pause between each check
			time.Sleep(100 * time.Millisecond)
		}
		// ok, we hit the last registration. We should expect to find the name for
		// another registerDur and then it should go away
		timer := time.NewTimer(time.Duration(registerDur) * time.Millisecond)
		startLastIter := time.Now()

		i = 0
	LASTITER:
		for {
			i = i + 1
			select {
			case <-timer.C:
				// ok, now we should expect it to go away
				break LASTITER
			default:
			}
			// check that we find the entry
			var found bool
			list, err := rm.GetMembers(ctx)
			if err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"name":       name,
					"numEntries": len(list),
					"iteration":  i,
				}).Warn("in lastiter caught error from get members")
			}
			for _, entry := range list {
				if entry == name {
					found = true
					// logrus.WithField("name", name).Info("lastiter: found entry")
					break
				}
			}
			if !found {
				dur := time.Since(startLastIter)
				logrus.WithFields(logrus.Fields{
					"name":             name,
					"numEntries":       len(list),
					"timeInLastIterMs": float64(dur / time.Millisecond),
					"expectedDurMs":    registerDur,
					"deltaExpire":      float64(registerDur) - float64(dur/time.Millisecond),
				}).Info("lastiter: Entry vanished")
				if float64(registerDur)-float64(dur/time.Millisecond) > 1600 {
					logrus.WithFields(logrus.Fields{
						"iteration":        i,
						"name":             name,
						"numEntries":       len(list),
						"timeInLastIterMs": float64(dur / time.Millisecond),
						"expectedDurMs":    registerDur,
						"deltaExpire":      float64(registerDur) - float64(dur/time.Millisecond),
					}).Warn("delta exceeded 1.6 seconds - out of bounds of expected expiration")
				}
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		// ok, we're beyond the duration of the last registration interval; now or
		// very soon we should see the entry disappear.
		i = 0
		for {
			// check that we find the entry
			var found bool
			list, err := rm.GetMembers(ctx)
			if err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"name":       name,
					"numEntries": len(list),
					"iteration":  i,
				}).Warn("in endgame caught error from get members")
			}
			for _, entry := range list {
				if entry == name {
					found = true
					break
				}
			}
			if !found {
				// we're done, the register is gone
				logrus.WithField("count", i).WithField("name", name).Infof("entry now gone")
				break
			}
			if i > 100 {
				logrus.WithField("name", name).Warn("entry still exists after 100 checks")
			}
			time.Sleep(10 * time.Millisecond)
		}
		logrus.WithField("name", name).Infof("all done checking entry")
	}()

}

// adds two entries with various sleeps and verifies they're there at the
// expected times
func linearTest(rm *redimem.RedisMembership) {
	ctx := context.Background()
	logrus.Infoln("about to register one for 3sec")
	rm.Register(ctx, "one", 3*time.Second)

	logrus.Infoln("about to sleep for 2sec")
	time.Sleep(2 * time.Second)

	logrus.Infoln("checking for one")
	list, _ := rm.GetMembers(ctx)
	spew.Dump(list)

	logrus.Infoln("about to register two for 3sec")
	rm.Register(ctx, "two", 3*time.Second)

	logrus.Infoln("checking for one and two")
	list, _ = rm.GetMembers(ctx)
	spew.Dump(list)

	logrus.Infoln("about to sleep for 1.5sec")
	time.Sleep(1500 * time.Millisecond)

	logrus.Infoln("checking list; one should be missing, two should be there")
	list, _ = rm.GetMembers(ctx)
	spew.Dump(list)

	logrus.Infoln("about to re-register two for 3sec")
	rm.Register(ctx, "two", 3*time.Second)

	logrus.Infoln("about to sleep for 2sec")
	time.Sleep(2 * time.Second)

	logrus.Infoln("checking list; one should be missing, two should be there")
	list, _ = rm.GetMembers(ctx)
	spew.Dump(list)

	logrus.Infoln("about to sleep for 1.5sec")
	time.Sleep(1500 * time.Millisecond)

	logrus.Infoln("checking list; both should be missing")
	list, _ = rm.GetMembers(ctx)
	spew.Dump(list)
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenID returns a random string of length numChars
func GenID(numChars int) string {
	id := make([]byte, numChars)
	for i := 0; i < numChars; i++ {
		id[i] = charset[rand.Intn(len(charset))]
	}
	return string(id)
}
