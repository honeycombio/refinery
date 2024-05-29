package peer

import (
	"testing"
	"time"

	"github.com/honeycombio/refinery/internal/gossip"
	"github.com/honeycombio/refinery/logger"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeers(t *testing.T) {
	clock := clockwork.NewFakeClock()
	gossip := &gossip.InMemoryGossip{}
	gossip.Start()
	peer1 := &PeerStore{
		identification: "peer1",
		Clock:          clock,
		Gossip:         gossip,
	}

	peer2 := &PeerStore{
		identification: "peer2",
		Clock:          clock,
		Gossip:         gossip,
	}
	require.NoError(t, peer1.Start())
	require.NoError(t, peer2.Start())

	peer1Chan := peer1.Subscribe()
	peer2Chan := peer2.Subscribe()

	peer1.PublishPeerInfo(PeerInfo{
		Data: []byte("peer1"),
	})

	peer2.PublishPeerInfo(PeerInfo{
		Data: []byte("peer2"),
	})

	for msg := range peer1Chan {
		require.Equal(t, []byte("peer2"), msg.Data)
		break
	}

	for msg := range peer2Chan {
		require.Equal(t, []byte("peer1"), msg.Data)
		break
	}

	require.Equal(t, 2, peer1.GetPeerCount())
	require.Equal(t, 2, peer2.GetPeerCount())
}

func TestPeer_GetPeerCount(t *testing.T) {
	// be able to get the count of peers
	peer1Clock := clockwork.NewFakeClock()
	gossip := &gossip.InMemoryGossip{
		Logger: &logger.NullLogger{},
	}
	gossip.Start()
	peer1 := &PeerStore{
		identification: "peer1",
		Clock:          peer1Clock,
		Gossip:         gossip,
	}

	peer2Clock := clockwork.NewFakeClock()
	peer2 := &PeerStore{
		identification: "peer2",
		Clock:          peer2Clock,
		Gossip:         gossip,
	}
	require.NoError(t, peer1.Start())
	require.NoError(t, peer2.Start())

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		peer1Clock.Advance(2 * keepAliveInterval)
		peer2Clock.Advance(2 * keepAliveInterval)

		assert.Equal(collect, 2, peer1.GetPeerCount())
		assert.Equal(collect, 2, peer2.GetPeerCount())

	}, 200*time.Millisecond, 10*time.Millisecond)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		peer1Clock.Advance(10 * peerEntryTimeout)
		require.Equal(t, 1, peer1.GetPeerCount())
	}, 100*time.Millisecond, 10*time.Millisecond)
}
