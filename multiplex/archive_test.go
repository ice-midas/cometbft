package multiplex_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/statesync"
	"github.com/cometbft/cometbft/types"
)

type TestConsensusNodesShutdownFn func(
	[]*config.Config,
	[]mx.MultiplexMap[*node.Node],
)

// ----------------------------------------------------------------------------
// Unit tests

func TestMultiplexSetupArchiveNewArchive(t *testing.T) {
	// Change this to create more nodes, or wait for more blocks
	numNodesPerChain := 2
	waitForNumBlocks := 1 // set to 0 to proceed without waiting

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfgs,
		testMultiplexes,
		testReactors,
		_, // committedBlocks
		shutdownRoutine := ResetTestMultiplexStartHistoricalNodeNeighborhood(t,
		numNodesPerChain,
		waitForNumBlocks,
		cmtlog.NewNopLogger(),
	)

	// Shutdown routine
	defer shutdownRoutine(globalCfgs, testMultiplexes)

	testChainId := testReactors[0].GetNetworks()[0]
	testNode0 := testMultiplexes[0][testChainId].GetInstance().(*node.Node)
	testNode1 := testMultiplexes[1][testChainId].GetInstance().(*node.Node)

	expectedSeedNodes := strings.Join([]string{
		requireGetNetAddress(t, testReactors[0].GetNodeKey(), testNode0.Config()),
		requireGetNetAddress(t, testReactors[1].GetNodeKey(), testNode1.Config()),
	}, ",")

	multiplexConfig := testReactors[0].GetNodeConfig().MultiplexConfig

	// Forces the two nodes above as ChainSeeds for the historical node
	multiplexConfig.ChainSeeds = map[string]string{
		testChainId: expectedSeedNodes,
	}

	// ---------------------
	// Now setup an archive

	rootDir,
		archiveCfg,
		archive := assertStartArchiveService(t,
		multiplexConfig,
		expectedSeedNodes,
		cmtlog.NewNopLogger(),
	)
	defer func() {
		archive.Stop()
		os.RemoveAll(rootDir)
	}()

	assert.Equal(t, rootDir, archiveCfg.RootDir)

	actualSeeds, err := archive.GetChainRegistry().GetSeeds(testChainId)
	assert.NoError(t, err, "should not error retrieving seed nodes for archive")
	assert.Equal(t, expectedSeedNodes, actualSeeds)
}

func TestMultiplexSetupArchiveConfigureSync(t *testing.T) {
	// Change this to create more nodes, or wait for more blocks
	numNodesPerChain := 2
	waitForNumBlocks := 1 // set to 0 to proceed without waiting

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfgs,
		testMultiplexes,
		testReactors,
		_, // committedBlocks
		shutdownRoutine := ResetTestMultiplexStartHistoricalNodeNeighborhood(t,
		numNodesPerChain,
		waitForNumBlocks,
		cmtlog.NewNopLogger(),
	)

	// Shutdown routine
	defer shutdownRoutine(globalCfgs, testMultiplexes)

	testChainId := testReactors[0].GetNetworks()[0]
	testNode0 := testMultiplexes[0][testChainId].GetInstance().(*node.Node)
	testNode1 := testMultiplexes[1][testChainId].GetInstance().(*node.Node)

	expectedSeedNodes := strings.Join([]string{
		requireGetNetAddress(t, testReactors[0].GetNodeKey(), testNode0.Config()),
		requireGetNetAddress(t, testReactors[1].GetNodeKey(), testNode1.Config()),
	}, ",")

	multiplexConfig := testReactors[0].GetNodeConfig().MultiplexConfig

	// Forces the two nodes above as ChainSeeds for the historical node
	multiplexConfig.ChainSeeds = map[string]string{
		testChainId: expectedSeedNodes,
	}

	// ---------------------
	// Now setup an archive

	rootDir,
		archiveCfg,
		archive := assertStartArchiveService(t,
		multiplexConfig,
		expectedSeedNodes,
		cmtlog.NewNopLogger(),
	)
	defer func() {
		archive.Stop()
		os.RemoveAll(rootDir)
	}()

	abciClient := assertStartArchiveABCI(t,
		archive.GetNetworks(),
		archiveCfg,
		cmtlog.NewNopLogger(),
	)

	// Archive: ABCI; ABCI: Archive.
	archive.SetABCIClient(abciClient)

	// Tests the initStateSync() internal method
	err := archive.ConfigureSync()
	assert.NoError(t, err, "should not error configuring state-sync")

	stateSyncReactors := archive.GetStateSyncReactors()
	assert.NotNil(t, stateSyncReactors)
	assert.Len(t, stateSyncReactors, len(archive.GetNetworks()))

	// Type-assertion to make sure we use correct state-sync reactors
	for _, chainId := range archive.GetNetworks() {
		assert.Contains(t, stateSyncReactors, chainId)
		instanceProvider := stateSyncReactors[chainId]
		assert.NotNil(t, instanceProvider)

		stateSyncReactor, ok := instanceProvider.GetInstance().(*statesync.Reactor)
		assert.Equal(t, true, ok)
		assert.NotNil(t, stateSyncReactor)
	}
}

func TestMultiplexSetupArchiveConfigureSyncErrors(t *testing.T) {
	// Change this to create more nodes, or wait for more blocks
	numNodesPerChain := 2
	waitForNumBlocks := 1 // set to 0 to proceed without waiting

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfgs,
		testMultiplexes,
		testReactors,
		_, // committedBlocks
		shutdownRoutine := ResetTestMultiplexStartHistoricalNodeNeighborhood(t,
		numNodesPerChain,
		waitForNumBlocks,
		cmtlog.NewNopLogger(),
	)

	// Shutdown routine
	defer shutdownRoutine(globalCfgs, testMultiplexes)

	testChainId := testReactors[0].GetNetworks()[0]
	testNode0 := testMultiplexes[0][testChainId].GetInstance().(*node.Node)
	testNode1 := testMultiplexes[1][testChainId].GetInstance().(*node.Node)

	expectedSeedNodes := strings.Join([]string{
		requireGetNetAddress(t, testReactors[0].GetNodeKey(), testNode0.Config()),
		requireGetNetAddress(t, testReactors[1].GetNodeKey(), testNode1.Config()),
	}, ",")

	multiplexConfig := testReactors[0].GetNodeConfig().MultiplexConfig

	// Forces the two nodes above as ChainSeeds for the historical node
	multiplexConfig.ChainSeeds = map[string]string{
		testChainId: expectedSeedNodes,
	}

	// ---------------------
	// Now setup an archive

	rootDir,
		_,
		archive := assertStartArchiveService(t,
		multiplexConfig,
		expectedSeedNodes,
		cmtlog.NewNopLogger(),
	)
	defer func() {
		archive.Stop()
		os.RemoveAll(rootDir)
	}()

	// Should error when missing ABCI client
	err := archive.ConfigureSync()
	assert.Error(t, err, "should error when ABCI client is not configured")
}

func TestMultiplexSetupArchiveStartStateSync(t *testing.T) {
	// Change this to create more nodes, or wait for more blocks
	numNodesPerChain := 2
	waitForNumBlocks := 1

	// Initialize and START the nodes multiplex AND historical node
	// For debug, change the logger(s) to cmtlog.TestingLogger()
	rootDir,
		testArchive,
		testMultiplexes,
		testReactors,
		_, // testCommittedBlocks
		_, // testTrustedBlockHash
		shutdownRoutine := ResetTestMultiplexHistoricalNodeEnvironment(t,
		numNodesPerChain,
		waitForNumBlocks,
		cmtlog.NewNopLogger(), // consensus
		cmtlog.NewNopLogger(), // archive
	)

	// Shutdown routine
	defer shutdownRoutine(rootDir, testArchive)

	assert.NotNil(t, testArchive, "should create archive instance")
	assert.Len(t, testMultiplexes, numNodesPerChain)
	assert.Len(t, testReactors, numNodesPerChain)

	testChainId := testReactors[0].GetNetworks()[0]
	assert.NotEmpty(t, testChainId)

	stateSyncReactors := testArchive.GetStateSyncReactors()
	assert.NotNil(t, stateSyncReactors,
		"should have created state-sync reactors")
	assert.Len(t, stateSyncReactors, len(testArchive.GetNetworks()))
	assert.Contains(t, stateSyncReactors, testChainId)

	// err := testArchive.StartStateSync()
	// assert.NoError(t, err, "should start state-sync process")
}

// ----------------------------------------------------------------------------
// Helpers

// CAUTION: re-uses the [config.MultiplexConfig] object from consensus nodes.
// CAUTION: forces created consensus nodes as ChainSeeds for the archive.
// CAUTION: forces state-sync configuration to use latest committed block.
func ResetTestMultiplexHistoricalNodeEnvironment(
	t testing.TB,
	numNodesPerChain int,
	waitForNumBlocks int,
	consensusLogger cmtlog.Logger,
	archiveLogger cmtlog.Logger,
) (
	string, // rootDir
	*mx.Archive, // testArchive
	[]mx.MultiplexMap[*node.Node], // testMultiplexes
	[]*mx.Reactor, // testReactors
	map[string][]any, // lastCommittedBlocks
	string, // trustedBlockHash
	TestHistoricalNodeShutdownFn, // shutdownRoutine
) {
	t.Helper()

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfgs,
		testMultiplexes,
		testReactors,
		testCommittedBlocks,
		shutdownRoutine := ResetTestMultiplexStartHistoricalNodeNeighborhood(t,
		numNodesPerChain,
		waitForNumBlocks,
		consensusLogger,
	)

	testChainId := testReactors[0].GetNetworks()[0]
	require.NotEmpty(t, testChainId)

	require.Len(t, testMultiplexes, numNodesPerChain)
	require.Len(t, testReactors, numNodesPerChain)

	require.Contains(t, testMultiplexes[0], testChainId)
	require.Contains(t, testMultiplexes[1], testChainId)
	require.Contains(t, testCommittedBlocks, testChainId)
	require.Len(t, testCommittedBlocks[testChainId], waitForNumBlocks)

	testNode0 := testMultiplexes[0][testChainId].GetInstance().(*node.Node)
	require.NotNil(t, testNode0, "should create first consensus node")

	testNode1 := testMultiplexes[1][testChainId].GetInstance().(*node.Node)
	require.NotNil(t, testNode1, "should create second consensus node")

	// We overwrite config.MultiplexConfig.ChainSeeds
	expectedSeedNodes := strings.Join([]string{
		requireGetNetAddress(t, testReactors[0].GetNodeKey(), testNode0.Config()),
		requireGetNetAddress(t, testReactors[1].GetNodeKey(), testNode1.Config()),
	}, ",")

	// We start state-sync with the latest block committed by consensus nodes.
	// Every block is fine to start state-sync (snapshots interval=1).
	lastCommitted := testCommittedBlocks[testChainId][waitForNumBlocks-1]
	lastBlockEvent, ok := lastCommitted.(types.EventDataNewBlock)
	require.Equal(t, true, ok, "should have received a new block event")
	lastCommittedBlockID := lastBlockEvent.BlockID

	// We overwrite config.MultiplexConfig.SyncConfig
	expectedStateSyncConfig := testReactors[0].GetNodeConfig().StateSync
	expectedStateSyncConfig.Enable = true
	expectedStateSyncConfig.TrustHeight = int64(waitForNumBlocks)
	expectedStateSyncConfig.TrustHash = lastCommittedBlockID.Hash.String()
	expectedStateSyncConfig.RPCServers = []string{
		testNode0.Config().RPC.ListenAddress,
		testNode1.Config().RPC.ListenAddress,
	}

	multiplexConfig := testReactors[0].GetNodeConfig().MultiplexConfig

	// Forces the two nodes above as ChainSeeds for the historical node
	multiplexConfig.ChainSeeds = map[string]string{
		testChainId: expectedSeedNodes,
	}

	// Forces the state-sync configuration from above as SyncConfig
	multiplexConfig.SyncConfig = map[string]*config.StateSyncConfig{
		testChainId: expectedStateSyncConfig,
	}

	// ---------------------
	// Now setup an archive

	rootDir,
		archiveCfg,
		archive := assertStartArchiveService(t,
		multiplexConfig,
		expectedSeedNodes,
		archiveLogger,
	)

	require.NotEmpty(t, rootDir,
		"should create historical node filesystem")

	_, err := os.Stat(rootDir)
	require.NoError(t, err,
		"should create filesystem resources for archive")

	abciClient := assertStartArchiveABCI(t,
		archive.GetNetworks(),
		archiveCfg,
		archiveLogger,
	)

	// Archive: ABCI; ABCI: Archive.
	archive.SetABCIClient(abciClient)

	// Tests the initStateSync() internal method
	err = archive.ConfigureSync()
	require.NoError(t, err, "should not error configuring state-sync")

	stateSyncReactors := archive.GetStateSyncReactors()
	require.NotNil(t, stateSyncReactors)
	require.Len(t, stateSyncReactors, len(archive.GetNetworks()))

	// Type-assertion to make sure we use correct state-sync reactors
	for _, chainId := range archive.GetNetworks() {
		assert.Contains(t, stateSyncReactors, chainId)
		instanceProvider := stateSyncReactors[chainId]
		require.NotNil(t, instanceProvider)

		stateSyncReactor, ok := instanceProvider.GetInstance().(*statesync.Reactor)
		require.Equal(t, true, ok)
		require.NotNil(t, stateSyncReactor)
	}

	return rootDir, archive, testMultiplexes, testReactors, testCommittedBlocks, lastCommittedBlockID.Hash.String(), func(
		root string,
		arc *mx.Archive,
	) {
		// Shutdown routine for consensus nodes
		defer shutdownRoutine(globalCfgs, testMultiplexes)

		// Shutdown routing for historical node
		arc.Stop()
		os.RemoveAll(root)
	}
}

func ResetTestMultiplexStartHistoricalNodeNeighborhood(
	t testing.TB,
	numNodesPerChain int,
	waitForNumBlocks int,
	customLogger cmtlog.Logger,
) (
	[]*config.Config,
	[]mx.MultiplexMap[*node.Node],
	[]*mx.Reactor,
	map[string][]any,
	TestConsensusNodesShutdownFn,
) {
	// Initialize and START the nodes multiplex
	globalCfgs,
		testMultiplexes,
		testReactors,
		shutdownRoutine := requireStartMultipleConsensusNodes(t, numNodesPerChain, customLogger)
	require.NotNil(t, shutdownRoutine)

	assert.Len(t, globalCfgs, numNodesPerChain,
		"should create correct number of nodes configurations")
	assert.Len(t, testMultiplexes, numNodesPerChain,
		"should create correct number of nodes multiplexes")
	assert.Len(t, testReactors, numNodesPerChain,
		"should create correct number of multiplex reactors")

	testChainId := testReactors[0].GetNetworks()[0]
	assert.Contains(t, testMultiplexes[0], testChainId)
	assert.Contains(t, testMultiplexes[1], testChainId)

	// Type-assertion to make sure we run the correct node instances
	node0 := testMultiplexes[0][testChainId].GetInstance().(*node.Node)
	node0Addr := requireGetNetAddress(t, testReactors[0].GetNodeKey(), node0.Config())
	require.NotEmpty(t, node0Addr)

	// Also test the second node instance and retrieve net address
	node1 := testMultiplexes[1][testChainId].GetInstance().(*node.Node)
	node1Addr := requireGetNetAddress(t, testReactors[1].GetNodeKey(), node1.Config())
	require.NotEmpty(t, node1Addr)

	// Check that we are using *different* net addresses
	assert.NotEqual(t, node0Addr, node1Addr,
		"should use different p2p net addresses for multiple nodes")

	// Waits for the first node to create X blocks
	committedBlocks := map[string][]any{}
	if waitForNumBlocks > 0 {
		committedBlocks = assertWaitForNodesMultiplexToProduceBlocks(t,
			testReactors[0],
			testMultiplexes[0],
			waitForNumBlocks, // expects x blocks
		)
		require.NotNil(t, committedBlocks)
		require.NotEmpty(t, committedBlocks)
		require.Contains(t, committedBlocks, testChainId)
		require.NotEmpty(t, committedBlocks[testChainId])
	}

	return globalCfgs, testMultiplexes, testReactors, committedBlocks, shutdownRoutine
}

// ----------------------------------------------------------------------------

func requireGetNetAddress(
	t testing.TB,
	nodeKey *p2p.NodeKey,
	nodeCfg *config.Config,
) string {
	t.Helper()

	laddr, err := p2p.NewNetAddressString(p2p.IDAddressString(
		nodeKey.ID(),
		nodeCfg.P2P.ListenAddress,
	))
	require.NoError(t, err, "should not error creating a p2p NetAddress for nodes")

	return laddr.String()
}

func requireStartMultipleConsensusNodes(
	t testing.TB,
	numNodesPerChain int,
	customLogger cmtlog.Logger,
) (
	[]*config.Config,
	[]mx.MultiplexMap[*node.Node],
	[]*mx.Reactor,
	TestConsensusNodesShutdownFn,
) {
	// Create a first multiplex and later re-use same network info
	// This generation creates a RANDOM nodes multiplex with numChains networks.
	globalCfg,
		testMultiplex,
		testReactor := assertStartNodesMultiplex(t, 1, customLogger) // 1 NETWORK

	require.NotNil(t, testReactor)
	testChainId := testReactor.GetNetworks()[0]
	require.Contains(t, testMultiplex, testChainId)

	node0 := testMultiplex[testChainId].GetInstance().(*node.Node)
	seedAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(
		testReactor.GetNodeKey().ID(),
		node0.Config().P2P.ListenAddress,
	))
	require.NoError(t, err, "should create net address for first node")

	configs := []*config.Config{globalCfg}
	nodesMx := []mx.MultiplexMap[*node.Node]{testMultiplex}
	reactors := []*mx.Reactor{testReactor}

	// First network's configuration is re-used
	for i := 1; i < numNodesPerChain; i++ {
		sameNetworkConfig := node0.Config().MultiplexConfig
		chainSeeds := seedAddr.String()
		require.NotEmpty(t, chainSeeds)

		// Ports overwrite configuration can't be re-used on same machine
		sameNetworkConfig.P2PStartPort = sameNetworkConfig.P2PStartPort + 1
		sameNetworkConfig.RPCStartPort = sameNetworkConfig.RPCStartPort + 1

		metricsPrefix := "cometbft" + strconv.Itoa(i) + ":" + t.Name()

		// Create subsequent multiplex and re-use first network info
		// This generation uses the network configuration from the first multiplex.
		globalCfg,
			testMultiplex,
			testReactor := assertStartNodesMultiplexWithConfig(t, sameNetworkConfig, customLogger, metricsPrefix, chainSeeds)

		require.NotNil(t, testReactor)
		require.Contains(t, testMultiplex, testChainId)

		configs = append(configs, globalCfg)
		nodesMx = append(nodesMx, testMultiplex)
		reactors = append(reactors, testReactor)
	}

	return configs, nodesMx, reactors, func(
		nodeCfgs []*config.Config,
		nodesMultiplexes []mx.MultiplexMap[*node.Node],
	) {
		for i, nodeCfg := range nodeCfgs {
			defer os.RemoveAll(nodeCfg.RootDir)
			for _, nodeInstance := range nodesMultiplexes[i] {
				ni := nodeInstance.GetInstance().(*node.Node)
				_ = ni.Stop()
			}
		}
	}
}

func assertWaitForNodesMultiplexToProduceBlocks(
	t testing.TB,
	reactor *mx.Reactor,
	nodesMultiplex mx.MultiplexMap[*node.Node],
	expectedBlocks int,
) map[string][]any {
	if expectedBlocks == 0 {
		return map[string][]any{} // Nothing to do
	}

	wg := sync.WaitGroup{}
	wg.Add(len(reactor.GetNetworks()))

	actualNumBlocks := make(map[string]int, len(reactor.GetNetworks()))
	committedBlocks := make(map[string][]any, len(reactor.GetNetworks()))

	for _, testChainId := range reactor.GetNetworks() {
		// Test that we have the correct node instance
		assert.Contains(t, nodesMultiplex, testChainId)
		assert.NotNil(t, nodesMultiplex[testChainId])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := nodesMultiplex[testChainId].GetInstance().(*node.Node)

		// Parallel goroutines with internal blocks loops
		go func(the_chain string, the_node *node.Node, maxBlocks int) {
			// Wait for the node to produce blocks
			blocksSub, err := the_node.EventBus().Subscribe(
				context.Background(),
				"node_test",
				types.EventQueryNewBlock,
			)
			assert.NoError(t, err)

			committedBlocks[the_chain] = make([]any, maxBlocks)
			numBlocks := 0

		NODE_BLOCKS_LOOP:
			for {
				select {
				case blockMsg := <-blocksSub.Out():
					committedBlocks[the_chain][numBlocks] = blockMsg.Data()
					numBlocks++
					if numBlocks == maxBlocks {
						actualNumBlocks[the_chain] = numBlocks
						wg.Done()
						break NODE_BLOCKS_LOOP
					}
				case <-blocksSub.Canceled():
					wg.Done()
					break NODE_BLOCKS_LOOP
				case <-time.After(15 * time.Second):
					wg.Done()
					break NODE_BLOCKS_LOOP
				}
			}
		}(testChainId, nodeInstance, expectedBlocks)
	}

	// Wait for all nodes to produce 3 blocks in parallel
	wg.Wait()

	// We assert that all networks produced at least 3 blocks, if an error
	// occurred on one of the networks, the map entry won't exist.
	for _, chainId := range reactor.GetNetworks() {
		assert.Contains(t, actualNumBlocks, chainId)
		assert.Contains(t, committedBlocks, chainId)

		assert.Equal(t, expectedBlocks, actualNumBlocks[chainId])
		assert.Len(t, committedBlocks[chainId], expectedBlocks,
			"should store committed blocks")
	}

	return committedBlocks
}

func assertStartArchiveService(
	t testing.TB,
	multiplexConfig config.MultiplexConfig,
	archiveSeedNodes string,
	customLogger cmtlog.Logger,
) (string, *config.Config, *mx.Archive) {
	rootDir, err := os.MkdirTemp("", t.Name()+"-archive_node")
	require.NoError(t, err)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = multiplexConfig

	metricsPrefix := "cometbft:archive:" + t.Name()
	nodeCfg.Instrumentation.Namespace = metricsPrefix

	// We always *ENABLE* state-sync for historical nodes
	for chainId, _ := range nodeCfg.SyncConfig {
		// Forcefully ENABLE state-sync
		nodeCfg.SyncConfig[chainId].Enable = true
	}

	chainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig)
	require.NoError(t, err, "should create chain registry from multiplex config")

	nodeKey := makeRandomNodeKey()
	archive := mx.NewArchive(
		nodeKey,
		nodeCfg,
		customLogger,
		chainRegistry,
	)

	assert.Equal(t, nodeKey.ID(), archive.GetNodeKey().ID())

	testChainId := archive.GetNetworks()[0]

	// Make sure getting seeds by ChainID works correctly
	seedNodes, err := chainRegistry.GetSeeds(testChainId)
	assert.NoError(t, err)
	assert.NotEmpty(t, seedNodes)
	assert.Equal(t, archiveSeedNodes, seedNodes)

	// And can we start the archive service?
	err = archive.Start()
	assert.NoError(t, err, "should start Archive service")

	return rootDir, nodeCfg, archive
}

func assertStartArchiveABCI(
	t testing.TB,
	chainIds []string,
	archiveCfg *config.Config,
	customLogger cmtlog.Logger,
) proxy.ChainConns {
	// Start an ABCI client
	abciClient := proxy.NewMultiplexAppConn(
		chainIds,
		proxy.DefaultClientCreator(archiveCfg.ProxyApp, archiveCfg.ABCI, archiveCfg.DBDir()),
		proxy.PrometheusMetrics(archiveCfg.Instrumentation.Namespace),
	)
	abciClient.SetLogger(customLogger)
	err := abciClient.Start()
	require.NoError(t, err, "should start ABCI client with ChainConns interface")

	return abciClient
}
