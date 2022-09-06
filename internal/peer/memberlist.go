package peer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	ml "github.com/hashicorp/memberlist"
	"github.com/honeycombio/refinery/config"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type memberList struct {
	log        logrus.FieldLogger
	memberList *ml.Memberlist
	c          config.Config
	events     *eventDelegate
	mutex      sync.Mutex
}

func newMemberListPeers(ctx context.Context, c config.Config) (Peers, error) {
	log := logrus.WithField("category", "member-list")

	// Get peer listener port.
	addr, err := c.GetPeerListenAddr()
	if err != nil {
		return nil, errors.Wrap(err, "error getting peer listener port from config")
	}

	_, portStr, err := net.SplitHostPort(addr)
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing peer listener port: %s", portStr)
	}

	m := &memberList{
		log: log,
		events: &eventDelegate{
			peers: make(map[string]struct{}, 1),
			c:     c,
			port:  uint32(port),
			log:   log,
		},
		c: c,
	}

	// Create the member list config
	mlc, err := m.newMLConfig()
	if err != nil {
		return nil, err
	}

	// Create a new member list instance
	m.memberList, err = ml.Create(mlc)
	if err != nil {
		return nil, err
	}

	// Attempt to join the member list using a list of known nodes
	for {
		_, err = m.memberList.Join(m.c.GetMemberListKnownMembers())
		if err != nil {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("while attempting to join member list: %#v: %w",
					m.c.GetMemberListKnownMembers(), err)
			case <-time.After(300 * time.Millisecond):
				continue
			}
		}
		break
	}

	return m, nil
}

func (m *memberList) newMLConfig() (*ml.Config, error) {
	// TODO(thrawn01): Allow users to provide a more complex member-list config besides
	//  just using the default config?
	config := ml.DefaultLANConfig()

	// NOTE: Typically, a member-list instance would fill out the `config.Delegate` with
	// some complex metadata in JSON/protobuf format to share with all members. However,
	// since refinery ONLY needs to know the `address:port` we overload the use of the
	// `Name` which is shared with all members for this purpose.

	// this is the address:port which refinery uses to communicate with other peers,
	// it is different from the address:port which member-list uses for gosip.
	config.Name, _ = m.c.GetPeerListenAddr()

	config.LogOutput = newLogWriter(m.log)
	config.PushPullInterval = 5 * time.Second

	addr := m.c.GetMemberListListenAddr()

	var err error
	config.BindAddr, config.BindPort, err = splitAddress(addr)
	if err != nil {
		return nil, fmt.Errorf("PeerManagement.ListenAddr `%s` is invalid: %w", addr, err)
	}

	config.AdvertiseAddr, config.AdvertisePort, err = splitAddress(m.c.GetMemberListAdvertiseAddr())
	if err != nil {
		return nil, fmt.Errorf("PeerManagement.AdvertiseAddr `%s` is invalid: %w",
			m.c.GetMemberListAdvertiseAddr(), err)
	}

	m.log.Debugf("PeerManagement.ListenAddr: %s Port: %d", config.BindAddr, config.BindPort)
	m.log.Debugf("PeerManagement.AdvertiseAddr: %s Port: %d", config.AdvertiseAddr, config.AdvertisePort)
	config.Events = m.events
	return config, nil
}

func (m *memberList) Close(ctx context.Context) error {
	errCh := make(chan error)
	go func() {
		if err := m.memberList.Leave(30 * time.Second); err != nil {
			errCh <- err
			return
		}
		errCh <- m.memberList.Shutdown()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
func (m *memberList) RegisterUpdatedPeersCallback(cb func()) {
	m.events.RegisterCallback(cb)
}

func (m *memberList) GetPeers() ([]string, error) {
	return m.events.GetPeers()
}

type eventDelegate struct {
	peers     map[string]struct{}
	log       logrus.FieldLogger
	c         config.Config
	port       uint32
	mutex     sync.Mutex
	callbacks []func()
}

func (e *eventDelegate) NotifyJoin(node *ml.Node) {
	defer e.mutex.Unlock()
	e.mutex.Lock()
	peerUrl := fmt.Sprintf("http://%s:%d", node.Addr.String(), e.port)
	e.log.WithField("peer", peerUrl).
		Infof("Peer join: %s", node.Name)
	e.peers[peerUrl] = struct{}{}
	e.callOnUpdate()
}

func (e *eventDelegate) NotifyLeave(node *ml.Node) {
	defer e.mutex.Unlock()
	e.mutex.Lock()
	peerUrl := fmt.Sprintf("http://%s:%d", node.Addr.String(), e.port)
	e.log.WithField("peer", peerUrl).
		Infof("Peer leave: %s", node.Name)
	delete(e.peers, peerUrl)
	e.callOnUpdate()
}

func (e *eventDelegate) NotifyUpdate(node *ml.Node) {
	defer e.mutex.Unlock()
	e.mutex.Lock()
	peerUrl := fmt.Sprintf("http://%s:%d", node.Addr.String(), e.port)
	e.log.WithFields(logrus.Fields{
		"status": node.State,
		"peer": peerUrl,
	}).
		Infof("Peer update: %s", node.Name)
	e.peers[peerUrl] = struct{}{}
	e.callOnUpdate()
}

func (e *eventDelegate) RegisterCallback(cb func()) {
	defer e.mutex.Unlock()
	e.mutex.Lock()
	e.callbacks = append(e.callbacks, cb)
}

func (e *eventDelegate) GetPeers() ([]string, error) {
	defer e.mutex.Unlock()
	e.mutex.Lock()
	var peers []string
	for k, _ := range e.peers {
		peers = append(peers, k)
	}

	// Sort the results since map will randomize the list items and
	// make comparing peer lists more difficult.
	sort.Slice(peers, func(i, j int) bool {
		return peers[i] < peers[j]
	})

	return peers, nil
}

func (e *eventDelegate) callOnUpdate() {
	for _, callback := range e.callbacks {
		// don't block on any of the callbacks.
		go callback()
	}
}

// newLogWriter pipes the output from the memberlist logger into logrus.
func newLogWriter(log logrus.FieldLogger) *io.PipeWriter {
	reader, writer := io.Pipe()

	go func() {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			log.Info(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			log.Errorf("Error while reading from Writer: %s", err)
		}
		reader.Close()
	}()
	runtime.SetFinalizer(writer, func(w *io.PipeWriter) {
		writer.Close()
	})

	return writer
}

func split(addr string) (string, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return host, 0, errors.New(" expected format is `address:port`")
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		return host, intPort, fmt.Errorf("port must be a number: %w", err)
	}
	return host, intPort, nil
}

func splitAddress(addr string) (string, int, error) {
	host, port, err := split(addr)
	if err != nil {
		return "", 0, err
	}
	// Member list requires the address to be an ip address
	if ip := net.ParseIP(host); ip == nil {
		addresses, err := net.LookupHost(host)
		if err != nil {
			return "", 0, fmt.Errorf("while preforming host lookup for '%s': %w", host, err)
		}
		if len(addresses) == 0 {
			return "", 0, fmt.Errorf("net.LookupHost() returned no addresses for '%s':%w", host, err)
		}
		host = addresses[0]
	}
	return host, port, nil
}
