package sharder

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWhichShard(t *testing.T) {
	const (
		selfPeerAddr = "127.0.0.1:8081"
		traceID      = "test"
	)

	peers := []string{
		"http://" + selfPeerAddr,
		"http://2.2.2.2:8081",
		"http://3.3.3.3:8081",
	}
	config := &config.MockConfig{
		GetPeerListenAddrVal: selfPeerAddr,
		GetPeersVal:          peers,
		PeerManagementType:   "file",
	}
	done := make(chan struct{})
	defer close(done)

	filePeers := &peer.FilePeers{Cfg: config, Metrics: &metrics.NullMetrics{}}
	require.NoError(t, filePeers.Start())

	sharder := DeterministicSharder{
		Config: config,
		Logger: &logger.NullLogger{},
		Peers:  filePeers,
	}

	assert.NoError(t, sharder.Start(),
		"starting deterministic sharder should not error")

	shard := sharder.WhichShard(traceID)
	assert.Contains(t, peers, shard.GetAddress(),
		"should select a peer for a trace")

	config.GetPeersVal = []string{}
	config.Reload()
	assert.Equal(t, shard.GetAddress(), sharder.WhichShard(traceID).GetAddress(),
		"should select the same peer if peer list becomes empty")
}

func TestWhichShardAtEdge(t *testing.T) {
	const (
		selfPeerAddr = "127.0.0.1:8081"
		traceID      = "RCIVNUNA" // carefully chosen (by trying over a billion times) to hash in WhichShard to 0xFFFFFFFF
	)

	// The algorithm in WhichShard works correctly for divisors of 2^32-1. The prime factorization of that includes
	// 1, 3, 5, 17, so we need something other than 3 to be sure that this test would fail.
	// It was tested (and failed) without the additional conditional.
	peers := []string{
		"http://" + selfPeerAddr,
		"http://2.2.2.2:8081",
		"http://3.3.3.3:8081",
		"http://4.4.4.4:8081",
	}

	config := &config.MockConfig{
		GetPeerListenAddrVal: selfPeerAddr,
		GetPeersVal:          peers,
		PeerManagementType:   "file",
	}
	done := make(chan struct{})
	defer close(done)

	filePeers := &peer.FilePeers{Cfg: config, Metrics: &metrics.NullMetrics{}}
	require.NoError(t, filePeers.Start())

	sharder := DeterministicSharder{
		Config: config,
		Logger: &logger.NullLogger{},
		Peers:  filePeers,
	}

	assert.NoError(t, sharder.Start(),
		"starting deterministic sharder should not error")

	shard := sharder.WhichShard(traceID)
	assert.Contains(t, peers, shard.GetAddress(),
		"should select a peer for a trace")

	config.GetPeersVal = []string{}
	config.Reload()
	assert.Equal(t, shard.GetAddress(), sharder.WhichShard(traceID).GetAddress(),
		"should select the same peer if peer list becomes empty")
}

// GenID returns a random hex string of length numChars
func GenID(numChars int) string {
	const charset = "abcdef0123456789"

	id := make([]byte, numChars)
	for i := 0; i < numChars; i++ {
		id[i] = charset[rand.Intn(len(charset))]
	}
	return string(id)
}

func BenchmarkShardBulk(b *testing.B) {
	const (
		selfPeerAddr = "127.0.0.1:8081"
		traceID      = "test"
	)

	const npeers = 11
	peers := []string{
		"http://" + selfPeerAddr,
	}
	for i := 1; i < npeers; i++ {
		peers = append(peers, fmt.Sprintf("http://2.2.2.%d/:8081", i))
	}
	config := &config.MockConfig{
		GetPeerListenAddrVal: selfPeerAddr,
		GetPeersVal:          peers,
		PeerManagementType:   "file",
	}
	done := make(chan struct{})
	defer close(done)

	filePeers := &peer.FilePeers{Cfg: config, Metrics: &metrics.NullMetrics{}}
	require.NoError(b, filePeers.Start())

	sharder := DeterministicSharder{
		Config: config,
		Logger: &logger.NullLogger{},
		Peers:  filePeers,
	}

	assert.NoError(b, sharder.Start(), "starting deterministic sharder should not error")

	const ntraces = 10
	ids := make([]string, ntraces)
	for i := 0; i < ntraces; i++ {
		ids[i] = GenID(32)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sharder.WhichShard(ids[i%ntraces])
	}
}

func TestShardBulk(t *testing.T) {
	const (
		selfPeerAddr = "127.0.0.1:8081"
		traceID      = "test"
	)

	// this test should work for a wide range of peer counts
	for i := 0; i < 5; i++ {
		npeers := i*10 + 5
		t.Run(fmt.Sprintf("bulk npeers=%d", npeers), func(t *testing.T) {
			for retry := 0; retry < 2; retry++ {
				peers := []string{
					"http://" + selfPeerAddr,
				}
				for i := 1; i < npeers; i++ {
					peers = append(peers, fmt.Sprintf("http://2.2.2.%d/:8081", i))
				}

				config := &config.MockConfig{
					GetPeerListenAddrVal: selfPeerAddr,
					GetPeersVal:          peers,
					PeerManagementType:   "file",
				}
				done := make(chan struct{})
				defer close(done)

				filePeers := &peer.FilePeers{Cfg: config, Metrics: &metrics.NullMetrics{}}
				require.NoError(t, filePeers.Start())

				sharder := DeterministicSharder{
					Config: config,
					Logger: &logger.NullLogger{},
					Peers:  filePeers,
				}

				assert.NoError(t, sharder.Start(), "starting sharder should not error")

				const ntraces = 1000
				ids := make([]string, ntraces)
				for i := 0; i < ntraces; i++ {
					ids[i] = GenID(32)
				}

				results := make(map[string]int)
				for i := 0; i < ntraces; i++ {
					s := sharder.WhichShard(ids[i])
					results[s.GetAddress()]++
				}
				min := ntraces
				max := 0
				for _, r := range results {
					if r < min {
						min = r
					}
					if r > max {
						max = r
					}
				}

				// This is probabilistic, so could fail, which is why we retry it once if it does.
				expectedResult := ntraces / npeers
				if min < expectedResult/3 || max > expectedResult*2 {
					if retry == 0 {
						t.Logf("probabalistic test failed once, retrying test with npeers=%d", npeers)
						continue
					}
					assert.Greater(t, expectedResult*2, max, "expected smaller max, got %d: %v", max, results)
					assert.NotEqual(t, expectedResult/3, min, "expected larger min, got %d: %v", min, results)
				} else {
					break // don't retry if it passed
				}
			}
		})
	}
}

func TestShardDrop(t *testing.T) {
	const (
		selfPeerAddr = "127.0.0.1:8081"
		traceID      = "test"
	)

	for i := 0; i < 5; i++ {
		npeers := i*10 + 5
		t.Run(fmt.Sprintf("drop npeers=%d", npeers), func(t *testing.T) {
			for retry := 0; retry < 2; retry++ {
				peers := []string{
					"http://" + selfPeerAddr,
				}
				for i := 1; i < npeers; i++ {
					peers = append(peers, fmt.Sprintf("http://2.2.2.%d/:8081", i))
				}

				config := &config.MockConfig{
					GetPeerListenAddrVal: selfPeerAddr,
					GetPeersVal:          peers,
					PeerManagementType:   "file",
				}
				done := make(chan struct{})
				defer close(done)

				filePeers := &peer.FilePeers{Cfg: config, Metrics: &metrics.NullMetrics{}}
				require.NoError(t, filePeers.Start())

				sharder := DeterministicSharder{
					Config: config,
					Logger: &logger.NullLogger{},
					Peers:  filePeers,
				}

				assert.NoError(t, sharder.Start(), "starting sharder should not error")

				type placement struct {
					id    string
					shard string
				}

				const ntraces = 1000
				placements := make([]placement, ntraces)
				for i := 0; i < ntraces; i++ {
					placements[i].id = GenID(32)
				}

				results := make(map[string]int)
				for i := 0; i < ntraces; i++ {
					s := sharder.WhichShard(placements[i].id)
					results[s.GetAddress()]++
					placements[i].shard = s.GetAddress()
				}

				// reach in and delete one of the peers, then reshard
				config.GetPeersVal = config.GetPeersVal[1:]
				sharder.loadPeerList()

				results = make(map[string]int)
				nDiff := 0
				for i := 0; i < ntraces; i++ {
					s := sharder.WhichShard(placements[i].id)
					results[s.GetAddress()]++
					if s.GetAddress() != placements[i].shard {
						nDiff++
					}
				}

				// we have a fairly large range here because it's truly random
				// and we've been having some flaky tests
				expected := ntraces / (npeers - 1)
				if nDiff < expected/2 || nDiff > expected*2 {
					if retry == 0 {
						t.Logf("probabalistic test failed once, retrying test with npeers=%d", npeers)
						continue
					}
					assert.Greater(t, expected*2, nDiff)
					assert.Less(t, expected/2, nDiff)
				} else {
					break // don't retry if it passed
				}
			}
		})
	}
}

func TestShardAddHash(t *testing.T) {
	const (
		selfPeerAddr = "127.0.0.1:8081"
		traceID      = "test"
	)

	for i := 0; i < 5; i++ {
		npeers := i*10 + 7
		t.Run(fmt.Sprintf("add npeers=%d", npeers), func(t *testing.T) {
			for retry := 0; retry < 2; retry++ {
				peers := []string{
					"http://" + selfPeerAddr,
				}
				for i := 1; i < npeers; i++ {
					peers = append(peers, fmt.Sprintf("http://2.2.2.%d/:8081", i))
				}

				config := &config.MockConfig{
					GetPeerListenAddrVal: selfPeerAddr,
					GetPeersVal:          peers,
					PeerManagementType:   "file",
				}
				done := make(chan struct{})
				defer close(done)

				filePeers := &peer.FilePeers{Cfg: config, Metrics: &metrics.NullMetrics{}}
				require.NoError(t, filePeers.Start())

				sharder := DeterministicSharder{
					Config: config,
					Logger: &logger.NullLogger{},
					Peers:  filePeers,
				}

				assert.NoError(t, sharder.Start(), "starting sharder should not error")

				type placement struct {
					id    string
					shard string
				}

				const ntraces = 1000
				placements := make([]placement, ntraces)
				for i := 0; i < ntraces; i++ {
					placements[i].id = GenID(32)
				}

				results := make(map[string]int)
				for i := 0; i < ntraces; i++ {
					s := sharder.WhichShard(placements[i].id)
					results[s.GetAddress()]++
					placements[i].shard = s.GetAddress()
				}

				// reach in and add a peer, then reshard
				config.GetPeersVal = append(config.GetPeersVal, "http://2.2.2.255/:8081")
				sharder.loadPeerList()

				results = make(map[string]int)
				nDiff := 0
				for i := 0; i < ntraces; i++ {
					s := sharder.WhichShard(placements[i].id)
					results[s.GetAddress()]++
					if s.GetAddress() != placements[i].shard {
						nDiff++
					}
				}
				expected := ntraces / (npeers - 1)
				// we have a fairly large range here because it's truly random
				// and we've been having some flaky tests
				if nDiff < expected/2 || nDiff > expected*2 {
					if retry == 0 {
						t.Logf("probabalistic test failed once, retrying test with npeers=%d", npeers)
						continue
					}
					assert.Greater(t, expected*2, nDiff)
					assert.Less(t, expected/2, nDiff)
				} else {
					break // don't retry if it passed
				}
				assert.Greater(t, expected*2, nDiff)
				assert.Less(t, expected/2, nDiff)
			}
		})
	}
}

func BenchmarkDeterministicShard(b *testing.B) {
	const (
		selfPeerAddr = "127.0.0.1:8081"
		traceID      = "test"
	)

	for i := 0; i < 5; i++ {
		npeers := i*10 + 4
		b.Run(fmt.Sprintf("benchmark_deterministic_%d", npeers), func(b *testing.B) {
			peers := []string{
				"http://" + selfPeerAddr,
			}
			for i := 1; i < npeers; i++ {
				peers = append(peers, fmt.Sprintf("http://2.2.2.%d/:8081", i))
			}
			config := &config.MockConfig{
				GetPeerListenAddrVal: selfPeerAddr,
				GetPeersVal:          peers,
				PeerManagementType:   "file",
			}
			done := make(chan struct{})
			defer close(done)

			filePeers := &peer.FilePeers{Cfg: config, Metrics: &metrics.NullMetrics{}}
			require.NoError(b, filePeers.Start())

			sharder := DeterministicSharder{
				Config: config,
				Logger: &logger.NullLogger{},
				Peers:  filePeers,
			}

			assert.NoError(b, sharder.Start(),
				"starting deterministic sharder should not error")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sharder.WhichShard(traceID)
			}
		})
	}
}
