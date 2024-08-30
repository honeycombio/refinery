package collect

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
)

const emaThroughputTopic = "ema_throughput"

// EMAThroughputCalculator encapsulates the logic to calculate a throughput value using an Exponential Moving Average (EMA).
type EMAThroughputCalculator struct {
	Config  config.Config   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	Clock   clockwork.Clock `inject:""`
	Pubsub  pubsub.PubSub   `inject:""`
	Peer    peer.Peers      `inject:""`

	throughputLimit uint
	weight          float64       // Smoothing factor for EMA
	intervalLength  time.Duration // Length of the interval
	hostID          string

	mut         sync.RWMutex
	throughputs map[string]throughputReport
	clusterEMA  uint
	eventCount  int // Internal count of events in the current interval
	done        chan struct{}
}

// NewEMAThroughputCalculator creates a new instance of EMAThroughputCalculator.
func (c *EMAThroughputCalculator) Start() error {
	cfg := c.Config.GetThroughputCalculatorConfig()
	c.throughputLimit = uint(cfg.Limit)
	c.done = make(chan struct{})

	// if throughput limit is not set, disable the calculator
	if c.throughputLimit == 0 {
		return nil
	}

	c.intervalLength = time.Duration(cfg.AdjustmentInterval)
	if c.intervalLength == 0 {
		c.intervalLength = 15 * time.Second
	}

	c.weight = cfg.Weight
	if c.weight == 0 {
		c.weight = 0.5
	}

	peerID, err := c.Peer.GetInstanceID()
	if err != nil {
		return err
	}
	c.hostID = peerID
	c.throughputs = make(map[string]throughputReport)

	c.Metrics.Register("cluster_throughput", "gauge")
	c.Metrics.Register("individual_throughput", "gauge")
	c.Metrics.Register("event_count_per_sec", "gauge")
	// Subscribe to the throughput topic so we can react to throughput
	// changes in the cluster.
	c.Pubsub.Subscribe(context.Background(), stressReliefTopic, c.onThroughputUpdate)

	go func() {
		ticker := c.Clock.NewTicker(c.intervalLength)
		for {
			select {
			case <-c.done:
				return
			case <-ticker.Chan():
				currentEMA := c.updateEMA()
				c.Pubsub.Publish(context.Background(), emaThroughputTopic, newThroughputMessage(currentEMA, peerID).String())
			}
		}

	}()

	return nil
}

func (c *EMAThroughputCalculator) onThroughputUpdate(ctx context.Context, msg string) {
	throughputMsg, err := unmarshalThroughputMessage(msg)
	if err != nil {
		return
	}
	c.mut.Lock()
	c.throughputs[throughputMsg.peerID] = throughputReport{
		key:        throughputMsg.peerID,
		throughput: throughputMsg.throughput,
		timestamp:  c.Clock.Now(),
	}
	c.mut.Unlock()
}

func (c *EMAThroughputCalculator) Stop() {
	close(c.done)
}

// IncrementEventCount increments the internal event count by a specified amount.
func (c *EMAThroughputCalculator) IncrementEventCount(count int) {
	c.mut.Lock()
	c.eventCount += count
	c.mut.Unlock()
}

// updateEMA calculates the current throughput and updates the EMA.
func (c *EMAThroughputCalculator) updateEMA() uint {
	c.mut.Lock()
	defer c.mut.Unlock()

	currentThroughput := float64(c.eventCount) / c.intervalLength.Seconds()
	c.Metrics.Gauge("event_count_per_sec", currentThroughput)

	report := throughputReport{
		key:        c.hostID,
		throughput: uint(currentThroughput),
		timestamp:  c.Clock.Now(),
	}
	c.throughputs[report.key] = report
	var totalThroughput float64

	for _, report := range c.throughputs {
		if report.key == c.hostID {
			totalThroughput += float64(report.throughput)
			continue
		}

		if c.Clock.Since(report.timestamp) > c.intervalLength*2 {
			delete(c.throughputs, report.key)
			continue
		}

		totalThroughput += float64(report.throughput)
	}
	c.clusterEMA = uint(math.Ceil(c.weight*totalThroughput + (1-c.weight)*float64(c.clusterEMA)))
	c.Metrics.Gauge("cluster_throughput", c.clusterEMA)

	c.eventCount = 0 // Reset the event count for the new interval

	return uint(currentThroughput)
}

// GetSamplingRateMultiplier calculates and returns a sampling rate multiplier
// based on the difference between the configured throughput limit and the current throughput.
func (c *EMAThroughputCalculator) GetSamplingRateMultiplier() float64 {
	if c.throughputLimit == 0 {
		return 1.0 // No limit set, so no adjustment needed
	}

	c.mut.RLock()
	currentEMA := c.clusterEMA
	c.mut.RUnlock()

	if currentEMA <= c.throughputLimit {
		return 1.0 // Throughput is within the limit, no adjustment needed
	}

	return float64(currentEMA) / float64(c.throughputLimit)
}

type throughputReport struct {
	key        string
	throughput uint
	timestamp  time.Time
}

type throughputMessage struct {
	peerID     string
	throughput uint
}

func newThroughputMessage(throughput uint, peerID string) *throughputMessage {
	return &throughputMessage{throughput: throughput, peerID: peerID}
}

func (msg *throughputMessage) String() string {
	return msg.peerID + "|" + fmt.Sprint(msg.throughput)
}

func unmarshalThroughputMessage(msg string) (*throughputMessage, error) {
	if len(msg) < 2 {
		return nil, fmt.Errorf("empty message")
	}

	parts := strings.SplitN(msg, "|", 2)
	throughput, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}

	return newThroughputMessage(uint(throughput), parts[0]), nil
}
