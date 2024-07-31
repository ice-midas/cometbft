package multiplex

import (
	//"fmt"
	"os"
	"testing"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cosmos/gogoproto/proto"

	p2pproto "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	"github.com/cometbft/cometbft/config"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
	"github.com/cometbft/cometbft/libs/log"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
)

func benchmarkSwitchBroadcast(b *testing.B, dbs []dbm.DB, switches []*p2p.Switch, numPairs int) {
	b.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()
	chMsg := &p2pproto.PexAddrs{
		Addrs: []p2pproto.NetAddress{
			{
				ID: "1",
			},
		},
	}

	b.SetParallelism(numPairs * 2) // 1 proc per switch
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should transport a message from switch 1 to switch 2
		for pb.Next() {
			switchIdx := randomizer.Intn(numPairs * 2)
			sw := switches[switchIdx]

			// Transport a message
			chID := byte(switchIdx % 4)
			sw.Broadcast(p2p.Envelope{ChannelID: chID, Message: chMsg})
		}
	})
}

// ----------------------------------------------------------------------------
// cometbft Switch implementation benchmarks

func BenchmarkP2PTransport1K(b *testing.B) {
	numPairs := 1000

	// Reset benchmark fs
	rootDir, dbPtrs, switchPtrs := ResetP2PTestRoot(b, "test-p2p-transport-1k", numPairs)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numPairs*2
	benchmarkSwitchBroadcast(b, dbPtrs, switchPtrs, numPairs)
}

// ----------------------------------------------------------------------------
// Mocks and Test Helpers

var cfg *config.P2PConfig

func init() {
	cfg = config.DefaultP2PConfig()
	cfg.PexReactor = true
	cfg.AllowDuplicateIP = true
}

type PeerMessage struct {
	Contents proto.Message
	Counter  int
}

type TestReactor struct {
	p2p.BaseReactor

	mtx          cmtsync.Mutex
	channels     []*conn.ChannelDescriptor
	logMessages  bool
	msgsCounter  int
	msgsReceived map[byte][]PeerMessage
}

func NewTestReactor(channels []*conn.ChannelDescriptor, logMessages bool) *TestReactor {
	tr := &TestReactor{
		channels:     channels,
		logMessages:  logMessages,
		msgsReceived: make(map[byte][]PeerMessage),
	}
	tr.BaseReactor = *p2p.NewBaseReactor("TestReactor", tr)
	tr.SetLogger(log.TestingLogger())
	return tr
}

func (tr *TestReactor) GetChannels() []*conn.ChannelDescriptor {
	return tr.channels
}

func (*TestReactor) AddPeer(p2p.Peer) {}

func (*TestReactor) RemovePeer(p2p.Peer, any) {}

func (tr *TestReactor) Receive(e p2p.Envelope) {
	if tr.logMessages {
		tr.mtx.Lock()
		defer tr.mtx.Unlock()
		//fmt.Printf("Received: %X, %X\n", e.ChannelID, e.Message)
		tr.msgsReceived[e.ChannelID] = append(tr.msgsReceived[e.ChannelID], PeerMessage{Contents: e.Message, Counter: tr.msgsCounter})
		tr.msgsCounter++
	}
}

func (tr *TestReactor) getMsgs(chID byte) []PeerMessage {
	tr.mtx.Lock()
	defer tr.mtx.Unlock()
	return tr.msgsReceived[chID]
}

// ----------------------------------------------------------------------------
// Exported helpers

func ResetP2PTestRoot(t testing.TB, testName string, numPairs int) (string, []dbm.DB, []*p2p.Switch) {
	t.Helper()

	// Creates many *prefixed* database instances using global db
	rootDir, dbPtrs := ResetPrefixDBTestRoot(t, "p2p_", numPairs)

	// Prepares slice of P2P switches
	switchPtrs := make([]*p2p.Switch, numPairs*2)

	// Creates many connected pairs of *P2P* switches
	for i := 0; i < numPairs; i++ {
		sw1, sw2 := MakeSwitchPair(initSwitchFn)

		t.Cleanup(func() {
			if err := sw2.Stop(); err != nil {
				t.Error(err)
			}
			if err := sw1.Stop(); err != nil {
				t.Error(err)
			}
		})

		idx := i * 2
		switchPtrs[idx] = sw1
		switchPtrs[idx+1] = sw2
	}

	return rootDir, dbPtrs, switchPtrs
}

// convenience method for creating two switches connected to each other.
// XXX: note this uses net.Pipe and not a proper TCP conn.
func MakeSwitchPair(initSwitch func(int, *p2p.Switch) *p2p.Switch) (*p2p.Switch, *p2p.Switch) {
	// Create two switches that will be interconnected.
	switches := p2p.MakeConnectedSwitches(cfg, 2, initSwitch, p2p.Connect2Switches)
	return switches[0], switches[1]
}

// ----------------------------------------------------------------------------

func initSwitchFn(_ int, sw *p2p.Switch) *p2p.Switch {
	sw.SetAddrBook(&p2p.AddrBookMock{
		Addrs:    make(map[string]struct{}),
		OurAddrs: make(map[string]struct{}),
	})

	// Make two reactors of two channels each
	sw.AddReactor("foo", NewTestReactor([]*conn.ChannelDescriptor{
		{ID: byte(0x00), Priority: 10, MessageType: &p2pproto.Message{}},
		{ID: byte(0x01), Priority: 10, MessageType: &p2pproto.Message{}},
	}, true))
	sw.AddReactor("bar", NewTestReactor([]*conn.ChannelDescriptor{
		{ID: byte(0x02), Priority: 10, MessageType: &p2pproto.Message{}},
		{ID: byte(0x03), Priority: 10, MessageType: &p2pproto.Message{}},
	}, true))

	return sw
}
