package multiplex_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/proxy"

	"github.com/cometbft/cometbft/internal/blocksync"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/internal/evidence"
	mempl "github.com/cometbft/cometbft/mempool"
)

func TestMultiplexReactorPrepareConsensusInstanceWithReactor(t *testing.T) {
	numChains := 5

	rootDir, globalCfg, reactor := ResetTestMultiplexConsensus(t,
		numChains,
	)
	defer os.RemoveAll(rootDir)

	// Start the reactor
	err := reactor.Start()
	require.NoError(t, err, "should start the multiplex reactor")

	err = reactor.WaitForNetworks()
	assert.NoError(t, err, "should not error while waiting for networks")

	// Start an ABCI client
	abciClient := proxy.NewMultiplexAppConn(
		reactor.GetNetworks(),
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.PrometheusMetrics(globalCfg.Instrumentation.Namespace),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(t, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Should now be able to do consensus handshake and load state machines
	for _, chainId := range reactor.GetNetworks() {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainId)
		assert.NoError(t, err, "should not error for consensus handshake")
	}
}

func TestMultiplexReactorCreateConsensusInstanceReactors(t *testing.T) {
	numChains := 5

	rootDir, globalCfg, reactor := ResetTestMultiplexConsensus(t,
		numChains,
	)
	defer os.RemoveAll(rootDir)

	// Start the reactor
	err := reactor.Start()
	require.NoError(t, err, "should start the multiplex reactor")

	err = reactor.WaitForNetworks()
	assert.NoError(t, err, "should not error while waiting for networks")

	// Start an ABCI client
	abciClient := proxy.NewMultiplexAppConn(
		reactor.GetNetworks(),
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.NopMetrics(),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(t, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Uses to retrieve reactors per network
	servicesProvider := reactor.GetServicesProvider()
	require.NotNil(t, servicesProvider, "services provider must not be nil")

	// Should now be able to do consensus handshake and load state machines
	for _, chainId := range reactor.GetNetworks() {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainId)
		assert.NoError(t, err, "should not error for consensus handshake")

		// Test with blockSync=true
		blockSync := true
		err = reactor.CreateConsensusInstanceReactors(
			context.TODO(),
			chainId,
			blockSync,
		)
		assert.NoError(t, err, "should not error creating consensus reactors")

		// Type-assertions make sure we have correct reactors set.
		testMempoolReactor := servicesProvider(mx.KEY_REACTOR_MEMPOOL, chainId).(*mempl.Reactor)
		testBlockSyncReactor := servicesProvider(mx.KEY_REACTOR_BLOCKSYNC, chainId).(*blocksync.Reactor)
		testConsensusReactor := servicesProvider(mx.KEY_REACTOR_CONSENSUS, chainId).(*cs.Reactor)
		testEvidenceReactor := servicesProvider(mx.KEY_REACTOR_EVIDENCE, chainId).(*evidence.Reactor)

		// Also make sure we have actual instances, not nil
		assert.NotNil(t, testMempoolReactor, "mempool reactor must not be nil")
		assert.NotNil(t, testBlockSyncReactor, "blockSync reactor must not be nil")
		assert.NotNil(t, testConsensusReactor, "consensus reactor must not be nil")
		assert.NotNil(t, testEvidenceReactor, "evidence reactor must not be nil")
	}
}

// CAUTION: the GenesisDocProvider is maleated to contain correct ChainIDs
// CAUTION: the MultiplexConfig is entirely random and *not synchronized* with genesis docs.
func ResetTestMultiplexConsensus(
	t testing.TB,
	numChains int,
) (string, *config.Config, *mx.Reactor) {
	t.Helper()

	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, numChains)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&nodeCfg.MultiplexConfig, numChains)

	nodeCfg.Instrumentation.Namespace = "cometbft:" + t.Name()

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(t, nodeCfg, mockGenesisProvider)

	return rootDir, nodeCfg, reactor
}
