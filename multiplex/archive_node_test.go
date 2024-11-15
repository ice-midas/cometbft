package multiplex_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/light"
	mx "github.com/cometbft/cometbft/multiplex"
)

type TestHistoricalNodeShutdownFn func(
	string,
	*mx.Archive,
)

// ----------------------------------------------------------------------------
// Unit tests

// CAUTION: waitForNumBlocks must be at least one to start state-sync with a non-zero height.
func TestMultiplexSetupArchiveWithStateSyncConfig(t *testing.T) {
	// Change this to create more nodes, or wait for more blocks
	numNodesPerChain := 2
	waitForNumBlocks := 1

	// Initialize and START the nodes multiplex AND historical node
	// For debug, change the logger(s) to cmtlog.TestingLogger()
	rootDir,
		testArchive,
		testMultiplexes,
		testReactors,
		testCommittedBlocks,
		testTrustedBlockHash,
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

	assert.Contains(t, testCommittedBlocks, testChainId)
	assert.Len(t, testCommittedBlocks[testChainId], waitForNumBlocks)

	// Make sure we have the correct state-sync configuration
	stateSyncConfig, err := testArchive.GetChainRegistry().GetStateSyncConfig(testChainId)
	assert.NoError(t, err, "should configure state-sync for historical node")

	assert.Equal(t, true, stateSyncConfig.Enable,
		"should enable state-sync for historical node")
	assert.Equal(t, int64(waitForNumBlocks), stateSyncConfig.TrustHeight,
		"should use last committed block height for state-sync")
	assert.Equal(t, testTrustedBlockHash, stateSyncConfig.TrustHash,
		"should use last committed block hash for state-sync")
	assert.NotEmpty(t, stateSyncConfig.RPCServers,
		"should use correct witness nodes for state-sync")
	assert.Len(t, stateSyncConfig.RPCServers, numNodesPerChain)
}

// CAUTION: waitForNumBlocks must be at least one to start state-sync with a non-zero height.
func TestMultiplexSetupArchiveCreateHistoricalLightClients(t *testing.T) {
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
		testTrustedBlockHash,
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

	// Execute test
	testLightClients, err := testArchive.CreateHistoricalLightClients(context.TODO())
	require.NoError(t, err, "should create light clients for replicated chains")
	assert.Contains(t, testLightClients, testChainId)

	// Do we have a light client?
	testLightClient, ok := testLightClients[testChainId].GetInstance().(*light.Client)
	assert.Equal(t, true, ok, "should create correct light client instances")

	// And is it possible to fetch blocks?
	testTrustedLightBlock, err := testLightClient.TrustedLightBlock(0) // 0 height for latest
	assert.NoError(t, err, "should not error retrieving latest light block")
	assert.NotNil(t, testTrustedLightBlock)

	// Uses the block hash received from RPC client
	actualTestTrustedBlockHash := testTrustedLightBlock.Commit.BlockID.Hash.String()
	assert.Equal(t, testTrustedBlockHash, actualTestTrustedBlockHash,
		"should fetch correct last committed block and use correct hash")
}

// CAUTION: waitForNumBlocks must be at least one to start state-sync with a non-zero height.
func TestMultiplexSetupArchiveCreateHistoricalLightClients_WithTrustedBlockHeight(t *testing.T) {
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
		testTrustedBlockHash,
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

	// Execute test
	testLightClients, err := testArchive.CreateHistoricalLightClients(context.TODO())
	require.NoError(t, err, "should create light clients for replicated chains")
	assert.Contains(t, testLightClients, testChainId)

	// Do we have a light client?
	testLightClient, ok := testLightClients[testChainId].GetInstance().(*light.Client)
	assert.Equal(t, true, ok, "should create correct light client instances")

	// And is it possible to fetch blocks by height?
	testTrustedLightBlock, err := testLightClient.TrustedLightBlock(int64(waitForNumBlocks)) // fetch by height
	assert.NoError(t, err, "should not error retrieving latest light block")
	assert.NotNil(t, testTrustedLightBlock)

	// Uses the block hash received from RPC client
	actualTestTrustedBlockHash := testTrustedLightBlock.Commit.BlockID.Hash.String()
	assert.Equal(t, testTrustedBlockHash, actualTestTrustedBlockHash,
		"should fetch correct last committed block and use correct hash")
}
