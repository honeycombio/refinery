package peer

import (
	"context"
	"errors"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-wyhash"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/jonboulle/clockwork"
)

// Registration timing.
const (
	// PeerEntryTimeout is how long we will wait before expiring a peer that
	// doesn't check in. The ratio of refresh to peer timeout should be about
	// 1/3; we overshoot because we add a jitter to the refresh interval.
	PeerEntryTimeout = 10 * time.Second

	// refreshCacheInterval is how frequently this host will re-register itself
	// by publishing their address. This should happen about 3x during each
	// timeout phase in order to allow multiple timeouts to fail and yet still
	// keep the host in the mix.
	refreshCacheInterval = 3 * time.Second

	// refreshCacheJitter is the most that a host adds to refreshCacheInterval.
	refreshCacheJitter = refreshCacheInterval / 5

	// maxRefreshCacheInterval is the longest a host waits between registrations
	// while its publishes succeed.
	maxRefreshCacheInterval = refreshCacheInterval + refreshCacheJitter
)

// Contact with peers and freshness of the peer list.
const (
	// peerMessageGapLimit is the longest gap between peer messages that still
	// counts as contact. It must be longer than a healthy gap
	// (maxRefreshCacheInterval) and short enough that a counted gap cannot
	// expire a live peer (PeerEntryTimeout less maxRefreshCacheInterval).
	peerMessageGapLimit = PeerEntryTimeout / 2

	// peersFreshDelay is the contact time a starting host waits before its list
	// is fresh: two registrations from each peer, so one lost registration does
	// not leave a peer out.
	peersFreshDelay = 2 * maxRefreshCacheInterval
)

// Health checks and warnings.
const (
	// peerManagementHealthKey is ready once this host has seen its own registration.
	peerManagementHealthKey = "peer_management"

	peerManagementHealthCheckInInterval = time.Second

	peerManagementHealthCheckInTimeout = 15 * time.Second

	noRegistrationWarnAfter = 30 * time.Second
)

type peerAction string

const (
	Register   peerAction = "R"
	Unregister peerAction = "U"
)

type peerCommand struct {
	action  peerAction
	id      string
	address string
}

func newPeerCommand(action peerAction, address string, id string) *peerCommand {
	return &peerCommand{
		action:  action,
		id:      id,
		address: address,
	}
}

// we want these commands to be short to minimize the amount of data we're
// sending over the wire, so we build a little protocol here:
// R<address>,<8-byte-id>  to register
// U<address>,<8-byte-id>  to unregister
// If we don't have an ID, then it's old-format Refinery and we just ignore it.
func (p *peerCommand) unmarshal(msg string) bool {
	idx := strings.Index(msg, ",")
	if len(msg) < 2 || idx == -1 {
		return false
	}
	// first letter indicates the action (eg register, unregister)
	p.action = peerAction(msg[:1])
	switch p.action {
	case Register, Unregister:
		// the remainder is the peer address and ID, separated by a comma
		msgData := msg[1:]
		p.address = msgData[:idx-1]
		p.id = msgData[idx:]
		return true
	default:
		return false
	}
}

// marshal returns a string representation of the peerCommand
// see unmarshal for the format
// we use this sprintf to ensure that we always get the right number of characters
func (p *peerCommand) marshal() string {
	return string(p.action) + p.address + "," + p.id
}

// contactClock only advances time when a peer message arrives, and then only
// by the time since the previous peer message, if that gap is at most
// peerMessageGapLimit. RedisPubsubPeers expires peer entries on it, so no entry
// expires while this host hears no peer messages. In that time it cannot tell
// a peer that stopped from a peer whose registrations do not reach it.
type contactClock struct {
	clock clockwork.Clock
	mut   sync.Mutex
	// lastMessage is when the last peer message arrived; zero before the first.
	lastMessage time.Time
	contactTime time.Duration
}

// Now returns the zero time plus the contact time. Only differences between
// its values mean anything.
func (c *contactClock) Now() time.Time {
	c.mut.Lock()
	defer c.mut.Unlock()
	return time.Time{}.Add(c.contactTime)
}

// messageArrived records a peer message that arrives now.
func (c *contactClock) messageArrived() {
	c.mut.Lock()
	defer c.mut.Unlock()
	// Read the clock under the lock, so that lastMessage only moves forward.
	now := c.clock.Now()
	if gap := now.Sub(c.lastMessage); !c.lastMessage.IsZero() && gap <= peerMessageGapLimit {
		c.contactTime += gap
	}
	c.lastMessage = now
}

// read returns when the last peer message arrived, and the contact time since
// the first one.
func (c *contactClock) read() (lastMessage time.Time, contactTime time.Duration) {
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.lastMessage, c.contactTime
}

var _ Peers = (*RedisPubsubPeers)(nil)

type RedisPubsubPeers struct {
	Config     config.Config   `inject:""`
	Metrics    metrics.Metrics `inject:"metrics"`
	Logger     logger.Logger   `inject:""`
	PubSub     pubsub.PubSub   `inject:""`
	Clock      clockwork.Clock `inject:""`
	Health     health.Recorder `inject:""`
	InstanceID string          `inject:"instanceID"`

	// Done is a channel that will be closed when the service should stop.
	// After it is closed, peers service should signal the rest of the cluster
	// that it is no longer available.
	// However, any messages send on the peers channel will still be processed
	// since the pubsub subscription is still active.
	Done chan struct{}

	peers   *generics.MapWithTTL[string, string]
	contact contactClock
	sub     pubsub.Subscription
	topic   string // formatted topic name

	// mut guards hash and callbacks: a pubsub runs subscription callbacks concurrently.
	mut       sync.Mutex
	hash      uint64
	callbacks []func()

	// lastSelfSeen is when this host's own Register message last came back
	// through the subscription; nil if none has. Store Clock.Now unchanged:
	// UTC, Local, In, Round, Truncate and encoding strip the monotonic reading.
	lastSelfSeen atomic.Pointer[time.Time]
}

var _ RegistrationAgeReporter = (*RedisPubsubPeers)(nil)

func (p *RedisPubsubPeers) RegistrationAge() (time.Duration, bool) {
	last := p.lastSelfSeen.Load()
	if last == nil {
		return 0, false
	}
	return p.Clock.Since(*last), true
}

var _ FreshnessReporter = (*RedisPubsubPeers)(nil)

func (p *RedisPubsubPeers) PeersAreFresh() (fresh, everFresh bool) {
	lastMessage, contactTime := p.contact.read()
	everFresh = p.lastSelfSeen.Load() != nil && contactTime >= peersFreshDelay
	return everFresh && p.Clock.Since(lastMessage) <= peerMessageGapLimit, everFresh
}

// checkHash checks the hash of the current list of peers and calls any registered callbacks
// in a separate goroutine if the hash has changed.
func (p *RedisPubsubPeers) checkHash() {
	peers := p.peers.SortedKeys()
	newhash := hashList(peers)

	p.mut.Lock()
	changed := newhash != p.hash
	var callbacks []func()
	if changed {
		p.hash = newhash
		callbacks = slices.Clone(p.callbacks)
	}
	p.mut.Unlock()

	for _, cb := range callbacks {
		go cb()
	}
	p.Metrics.Gauge("num_peers", float64(len(peers)))
	p.Metrics.Gauge("peer_hash", float64(newhash))
}

func (p *RedisPubsubPeers) listen(ctx context.Context, msg string) {
	cmd := &peerCommand{}
	if !cmd.unmarshal(msg) {
		return
	}
	p.Metrics.Count("peer_messages", 1)

	// Advance the clock before Set, so the entry's expiry counts from this message.
	p.contact.messageArrived()
	if cmd.action == Register && cmd.id == p.InstanceID {
		now := p.Clock.Now()
		p.lastSelfSeen.Store(&now)
	}

	switch cmd.action {
	case Unregister:
		p.peers.Delete(cmd.id)
	case Register:
		p.peers.Set(cmd.id, cmd.address)
	}
	p.checkHash()
}

var redisPubSubPeersMetrics = []metrics.Metadata{
	{Name: "num_peers", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "the active number of peers in the cluster"},
	{Name: "peer_hash", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "the hash of the current list of peers"},
	{Name: "peer_messages", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "the number of messages received by the peers service"},
	{Name: "peer_registration_age_ms", Type: metrics.Gauge, Unit: metrics.Milliseconds, Description: "milliseconds since this host last saw its own registration come back through Redis, or since it started if it has not seen one"},
}

func (p *RedisPubsubPeers) Start() error {
	if p.PubSub == nil {
		return errors.New("injected pubsub is nil")
	}
	// if we didn't get an injected logger or metrics, use the null ones (for tests)
	if p.Metrics == nil {
		p.Metrics = &metrics.NullMetrics{}
	}
	if p.Logger == nil {
		p.Logger = &logger.NullLogger{}
	}

	p.peers = generics.NewMapWithTTL[string, string](PeerEntryTimeout, nil)
	p.contact.clock = p.Clock
	p.peers.Clock = &p.contact
	p.callbacks = make([]func(), 0)

	p.topic = p.PubSub.FormatTopic("peers")

	p.Logger.Info().Logf("subscribing to pubsub peers channel")
	p.sub = p.PubSub.Subscribe(context.Background(), p.topic, p.listen)

	for _, metric := range redisPubSubPeersMetrics {
		p.Metrics.Register(metric)
	}

	myaddr, err := publicAddr(p.Logger, p.Config)
	if err != nil {
		return err
	}
	p.peers.Set(p.InstanceID, myaddr)

	p.Health.Register(peerManagementHealthKey, peerManagementHealthCheckInTimeout)
	return nil
}

func (p *RedisPubsubPeers) Ready() error {
	myaddr, err := publicAddr(p.Logger, p.Config)
	if err != nil {
		return err
	}

	// Report readiness from its own goroutine so a hung publish cannot stall
	// the reports and fail the liveness check.
	go func() {
		ticker := p.Clock.NewTicker(peerManagementHealthCheckInInterval)
		defer ticker.Stop()
		started := p.Clock.Now()
		warned := false
		for {
			select {
			case <-ticker.Chan():
				age, seen := p.RegistrationAge()
				p.Health.Ready(peerManagementHealthKey, seen)
				if !seen {
					age = p.Clock.Since(started)
				}
				p.Metrics.Gauge("peer_registration_age_ms", float64(age.Milliseconds()))
				if !seen && !warned && p.Clock.Since(started) >= noRegistrationWarnAfter {
					warned = true
					p.Logger.Warn().Logf("this host has not seen its own registration come back through Redis, so it is not ready; check that Redis is reachable, that the credentials are valid, and that the user may use both PUBLISH and SUBSCRIBE")
				}
			case <-p.Done:
				p.Health.Unregister(peerManagementHealthKey)
				return
			}
		}
	}()

	// periodically refresh our presence in the list of peers, and update peers as they come in
	go func() {
		// we want our refresh cache interval to vary from peer to peer so they
		// don't always hit redis at the same time, so we add a random jitter of up
		// to 20% of the interval
		interval := refreshCacheInterval + time.Duration(rand.Int63n(int64(refreshCacheJitter)))
		ticker := p.Clock.NewTicker(interval)
		defer ticker.Stop()

		// every 25-35 seconds, log the current state of the peers
		// (we could make this configurable if we wanted but it's not that important)
		logTicker := p.Clock.NewTicker((time.Duration(rand.Intn(10000))*time.Millisecond + (25 * time.Second)))
		defer logTicker.Stop()
		for {
			select {
			case <-p.Done:
				p.stop()
				return
			case <-ticker.Chan():

				// publish our presence periodically
				ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
				err := p.PubSub.Publish(ctx, p.topic, newPeerCommand(Register, myaddr, p.InstanceID).marshal())
				if err != nil {
					p.Logger.Error().WithFields(map[string]interface{}{
						"error":       err,
						"hostaddress": myaddr,
					}).Logf("failed to publish peer address")
				}
				cancel()
			case <-logTicker.Chan():
				p.mut.Lock()
				hash := p.hash
				p.mut.Unlock()
				p.Logger.Debug().WithFields(map[string]any{
					"ids":       p.peers.SortedKeys(),
					"peers":     p.peers.SortedValues(),
					"hash":      hash,
					"num_peers": p.peers.Length(),
					"self":      myaddr,
				}).Logf("peer report")
			}
		}
	}()

	return nil
}

// stop send a message to the pubsub channel to unregister this peer
// but it does not close the subscription.
func (p *RedisPubsubPeers) stop() {
	// unregister ourselves
	myaddr, err := publicAddr(p.Logger, p.Config)
	if err != nil {
		p.Logger.Error().Logf("failed to get public address")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.Config.GetPeerTimeout())
	defer cancel()
	err = p.PubSub.Publish(ctx, p.topic, newPeerCommand(Unregister, myaddr, p.InstanceID).marshal())
	if err != nil {
		p.Logger.Error().WithFields(map[string]interface{}{
			"error":       err,
			"hostaddress": myaddr,
		}).Logf("failed to publish peer address")
	}
}

func (p *RedisPubsubPeers) GetPeers() ([]string, error) {
	// we never want to return an empty list of peers, so if the system returns
	// an empty list, return a single peer (its name doesn't really matter).
	// This keeps the sharding logic happy.
	peers := p.peers.SortedValues()
	if len(peers) == 0 {
		myaddr, err := publicAddr(p.Logger, p.Config)
		if err != nil {
			return nil, err
		}
		peers = []string{myaddr}
	}
	return peers, nil
}

func (p *RedisPubsubPeers) GetInstanceID() (string, error) {
	return publicAddr(p.Logger, p.Config)
}

func (p *RedisPubsubPeers) RegisterUpdatedPeersCallback(callback func()) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.callbacks = append(p.callbacks, callback)
}

// hashList hashes a list of strings into a single uint64
func hashList(list []string) uint64 {
	var h uint64 = 255798297204 // arbitrary seed
	for _, s := range list {
		h = wyhash.Hash([]byte(s), h)
	}
	return h
}
