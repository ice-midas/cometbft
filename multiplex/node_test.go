package multiplex_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cs "github.com/cometbft/cometbft/internal/consensus"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmttest "github.com/cometbft/cometbft/internal/test"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/node"
	cmtnode "github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

// CAUTION: do not remove this test because it makes sure that that multiplex
// implementation *does not interfere* with the legacy node implementation.
func TestMultiplexNodeLegacyNodeImplementation(t *testing.T) {
	testChainId := "test-legacy-chain-id"
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Make sure we have /data and /config
	config.EnsureRoot(rootDir)

	// Make sure we have a *one-doc* genesis file (GenesisDoc)
	baseConfig := config.DefaultBaseConfig()
	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := fmt.Sprintf(testLegacyGenesisDocFmt, testChainId) // LEGACY!
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// Create a legacy Test configuration
	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)

	// Make sure we have a privValidator
	privValidator, err := privval.LoadOrGenFilePV(
		globalCfg.PrivValidatorKeyFile(),
		globalCfg.PrivValidatorStateFile(),
		useDefaultKeyGenFunc(),
	)
	require.NoError(t, err)

	n, err := cmtnode.NewNode(
		context.Background(),
		globalCfg,
		privValidator,
		makeRandomNodeKey(),
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		cmtnode.DefaultGenesisDocProviderFunc(globalCfg),
		config.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(globalCfg.Instrumentation),
		cmtlog.NewNopLogger(), // cmtlog.TestingLogger() more verbose
	)
	require.NoError(t, err)

	// Start and stop to test full run-up of node
	err = n.Start()
	defer n.Stop()
	assert.NoError(t, err)
}

func TestMultiplexNodeNewLegacyNodeMultiplex(t *testing.T) {
	testChainId := "test-legacy-chain-id"
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Make sure we have /data and /config
	config.EnsureRoot(rootDir)

	// Make sure we have a *one-doc* genesis file (GenesisDoc)
	baseConfig := config.DefaultBaseConfig()
	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := fmt.Sprintf(testLegacyGenesisDocFmt, testChainId) // LEGACY!
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// Create a legacy Test configuration
	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)

	// Create the [node.Node] instance, using [node.NewNode]
	// nil-Reactor instance is ignored
	testMultiplex, _, err := mx.NewLegacyNodeMultiplex(
		context.Background(),
		globalCfg,
		makeRandomNodeKey(),
		cmtlog.NewNopLogger(),
	)
	assert.NoError(t, err, "should create node instance")
	assert.NotNil(t, testMultiplex, "should return a multiplex map with a node")
	assert.Len(t, testMultiplex, 1, "should return a multiplex map with exactly one node")
	assert.Contains(t, testMultiplex, testChainId)
	assert.NotNil(t, testMultiplex[testChainId])

	// Type-assertion to verify that we have a correct instance
	legacyNode := testMultiplex[testChainId].GetInstance().(*node.Node)
	genesisDoc := legacyNode.GenesisDoc()

	// Verify that we are on the correct ChainID
	assert.Equal(t, testChainId, genesisDoc.ChainID)

	// Start and stop to close the db for later re-tests
	err = legacyNode.Start()
	defer legacyNode.Stop()
	require.NoError(t, err, "legacy node should start correctly")
}

func TestMultiplexNodeNewNodesMultiplexFallback(t *testing.T) {
	// We define the necessary infrastructure for a legacy node
	testChainId := "test-legacy-chain-id"
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Make sure we have /data and /config
	config.EnsureRoot(rootDir)

	// Make sure we have a *one-doc* genesis file (GenesisDoc)
	baseConfig := config.DefaultBaseConfig()
	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := fmt.Sprintf(testLegacyGenesisDocFmt, testChainId) // LEGACY!
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// Create a legacy Test configuration
	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)

	// Also generate a multiplex config *but* disable it using "disabled"
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 3)
	globalCfg.Strategy = mx.DisableReplicationStrategy()

	// The multiplex configuration will be ignored due to disabled flag.
	// Should create the [node.Node] instance, using [node.NewNode]
	testMultiplex, _, err := mx.NewNodesMultiplex(
		context.Background(),
		globalCfg,
		cmtlog.NewNopLogger(),
	)
	assert.NoError(t, err, "should create node instance")
	assert.NotNil(t, testMultiplex, "should return a multiplex map with a node")
	assert.Len(t, testMultiplex, 1, "should return a multiplex map with exactly one node")
	assert.Contains(t, testMultiplex, testChainId)
	assert.NotNil(t, testMultiplex[testChainId])

	// Type-assertion to verify that we have a correct instance
	legacyNode := testMultiplex[testChainId].GetInstance().(*node.Node)
	genesisDoc := legacyNode.GenesisDoc()

	// Verify that we are on the correct ChainID
	assert.Equal(t, testChainId, genesisDoc.ChainID)

	// Start and stop to close the db for later re-testing
	err = legacyNode.Start()
	defer legacyNode.Stop()
	require.NoError(t, err, "legacy node should start correctly")
}

func TestMultiplexNodeNewNodesMultiplex(t *testing.T) {
	numChains := 5
	rootDir, globalCfg := ResetTestMultiplexNode(t, numChains)
	defer os.RemoveAll(rootDir)

	// Forces multi-test allowance, disables GRPC
	globalCfg.Instrumentation.Namespace = "cometbft:" + t.Name()
	globalCfg.GRPC.ListenAddress = ""            // disabled GRPC
	globalCfg.GRPC.Privileged.ListenAddress = "" // disabled GRPC

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	testMultiplex, testReactor, err := mx.NewNodesMultiplex(
		context.Background(),
		globalCfg,
		cmtlog.NewNopLogger(),
	)
	assert.NoError(t, err, "should create node instance")
	assert.NotNil(t, testMultiplex, "should return a multiplex map with a node")
	assert.Len(t, testMultiplex, numChains, fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	configProvider := testReactor.GetInstanceProvider(mx.KEY_CONFIG)
	assert.NotNil(t, configProvider)

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	// Test that we have all the required networks
	for _, testChainId := range testReactor.GetNetworks() {
		assert.Contains(t, testMultiplex, testChainId)
		assert.NotNil(t, testMultiplex[testChainId])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainId].GetInstance().(*node.Node)
		genesisDoc := nodeInstance.GenesisDoc()
		cfgOverwrite := configProvider(testChainId).(*config.Config)

		stateSyncConf, err := testReactor.GetChainRegistry().GetStateSyncConfig(testChainId)
		assert.NoError(t, err, "should get state-sync configuration per network")
		assert.Equal(t, false, stateSyncConf.Enable, "state-sync should be disabled")

		// Verify that we are on the correct ChainID
		assert.Equal(t, testChainId, genesisDoc.ChainID)

		// Verify state-sync configuration
		assert.Equal(t, false, cfgOverwrite.StateSync.Enable, "state-sync should be disabled")

		// Verify that we can start the node correctly
		go func(cn *node.Node) {
			defer wg.Done()
			// t.Logf("Starting new node: %s", cn.GenesisDoc().ChainID)
			// t.Logf("Using listen addr: p2p:%s - rpc:%s", cn.Config().P2P.ListenAddress, cn.Config().RPC.ListenAddress)
			err := cn.Start()
			require.NoError(t, err)
		}(nodeInstance)
	}

	// Wait for both nodes to have produced a block
	//t.Logf("Waiting for %d nodes to be up and running.", len(testReactor.GetNetworks()))
	wg.Wait()

	// Shutdown routine
	defer func(nodesMultiplex mx.MultiplexMap[*node.Node]) {
		for _, nodeInstance := range nodesMultiplex {
			ni := nodeInstance.GetInstance().(*node.Node)
			_ = ni.Stop()
		}
	}(testMultiplex)
}

func TestMultiplexNodeNewNodesMultiplexSingleNetworkProduceBlocks(t *testing.T) {
	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfg,
		testMultiplex,
		testReactor := assertStartNodesMultiplex(t, 1, cmtlog.NewNopLogger()) // 1 NETWORK!

	// Shutdown routine
	defer func(nodesMultiplex mx.MultiplexMap[*node.Node]) {
		defer os.RemoveAll(globalCfg.RootDir)
		for _, nodeInstance := range nodesMultiplex {
			ni := nodeInstance.GetInstance().(*node.Node)
			_ = ni.Stop()
		}
	}(testMultiplex)

	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	expectedBlocks := 3
	actualNumBlocks := make(map[string]int, len(testReactor.GetNetworks()))

	for _, testChainId := range testReactor.GetNetworks() {
		// Test that we have the correct node instance
		assert.Contains(t, testMultiplex, testChainId)
		assert.NotNil(t, testMultiplex[testChainId])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainId].GetInstance().(*node.Node)

		// Parallel goroutines with internal blocks loops
		go func(the_chain string, the_node *node.Node, maxBlocks int) {
			// Wait for the node to produce blocks
			blocksSub, err := the_node.EventBus().Subscribe(
				context.Background(),
				"node_test",
				types.EventQueryNewBlock,
			)
			assert.NoError(t, err)

			numBlocks := 0

		NODE_BLOCKS_LOOP:
			for {
				select {
				case <-blocksSub.Out():
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
	for _, chainId := range testReactor.GetNetworks() {
		assert.Contains(t, actualNumBlocks, chainId)
		assert.Equal(t, expectedBlocks, actualNumBlocks[chainId])
	}
}

func TestMultiplexNodeNewNodesMultiplexProduceBlocks(t *testing.T) {
	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfg,
		testMultiplex,
		testReactor := assertStartNodesMultiplex(t, 5, cmtlog.NewNopLogger()) // 5 networks

	// Shutdown routine
	defer func(nodesMultiplex mx.MultiplexMap[*node.Node]) {
		defer os.RemoveAll(globalCfg.RootDir)
		for _, nodeInstance := range nodesMultiplex {
			ni := nodeInstance.GetInstance().(*node.Node)
			_ = ni.Stop()
		}
	}(testMultiplex)

	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	expectedBlocks := 3
	actualNumBlocks := make(map[string]int, len(testReactor.GetNetworks()))

	for _, testChainId := range testReactor.GetNetworks() {
		// Test that we have the correct node instance
		assert.Contains(t, testMultiplex, testChainId)
		assert.NotNil(t, testMultiplex[testChainId])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainId].GetInstance().(*node.Node)

		// Parallel goroutines with internal blocks loops
		go func(the_chain string, the_node *node.Node, maxBlocks int) {
			// Wait for the node to produce blocks
			blocksSub, err := the_node.EventBus().Subscribe(
				context.Background(),
				"node_test",
				types.EventQueryNewBlock,
			)
			assert.NoError(t, err)

			numBlocks := 0

		NODE_BLOCKS_LOOP:
			for {
				select {
				case <-blocksSub.Out():
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
	for _, chainId := range testReactor.GetNetworks() {
		assert.Contains(t, actualNumBlocks, chainId)
		assert.Equal(t, expectedBlocks, actualNumBlocks[chainId])
	}
}

// CAUTION: this test method sets up a random multiplex with a valid GenesisDocSet.
// CAUTION: this method forcefully *disables state-sync* to permit starting new networks.
func ResetTestMultiplexNode(t testing.TB, numChains int) (string, *config.Config) {
	t.Helper()

	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(t, numChains)

	// Make sure we *disable* state-sync ("new network", "first block")
	for chainId, _ := range globalCfg.SyncConfig {
		// Forcefully disable state-sync
		globalCfg.SyncConfig[chainId].Enable = false
	}

	// Make sure we have /data and /config
	_, err = mx.NewMultiplexFS(globalCfg)
	require.NoError(t, err, "should create filesystem structure for multiplex")

	// Make sure we have a *multi-doc* genesis file (GenesisDocSet)
	genesisFilePath := filepath.Join(rootDir, globalCfg.Genesis)

	// IMPORTANT:
	// If there is a genesis file at the configured path, we will read it and expect it
	// to contain a genesis doc set ; otherwise create it with testOneScopedGenesisFmt.

	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := `[`
		for userAddress, chainIds := range globalCfg.UserChains {
			for _, chainId := range chainIds {
				// Creates one genesis doc per pair of user address and chainId
				chainTestGenesis := fmt.Sprintf(testGenesisDocWithValidatorsFmt, chainId, testDefaultGenesisValidator)
				testGenesis += chainTestGenesis + ","

				// resets priv validators to default state/key (as present in genesis)
				// useDefaultPrivValidator=true
				ResetMultiplexPrivValidator(globalCfg.BaseConfig, userAddress, chainId, nil, true)
			}
		}

		// Removes last comma and closes json array
		testGenesis = testGenesis[:len(testGenesis)-1] + `]`
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	return rootDir, globalCfg
}

func ResetMultiplexPrivValidator(
	conf config.BaseConfig,
	userAddress string,
	chainId string,
	privValidator *privval.FilePV,
	useDefaultPrivValidator bool,
) *privval.FilePV {
	userConfDir := filepath.Join(conf.RootDir, config.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(conf.RootDir, config.DefaultDataDir, userAddress)

	privValKeyDir := filepath.Join(userConfDir, chainId)
	privValStateDir := filepath.Join(userDataDir, chainId)

	privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(conf.PrivValidatorKeyFile()))
	privValStateFile := filepath.Join(privValStateDir, filepath.Base(conf.PrivValidatorStateFile()))

	// fmt.Printf("Resetting priv validator key file: %s\n", privValKeyFile)
	// fmt.Printf("Resetting priv validator state file: %s\n", privValStateFile)

	if useDefaultPrivValidator {
		// CAUTION: careful this uses always the same priv validator,
		// if a change is made to it, please also update the genesis.validators.
		cmttest.ResetTestPrivValidatorFiles(privValKeyFile, privValStateFile)
		return privval.LoadFilePV(privValKeyFile, privValStateFile)
	}

	// Not using default priv validator, we will either generate a random
	// new priv validator key, or use the one provided with privValidator
	filePV := &privval.FilePV{}

	if privValidator == nil {
		// IMPORTANT: This generates a random privValidator private key
		// TODO(midas): should not ignore if an error is produced.
		filePV, _ = privval.GenFilePV(privValKeyFile, privValStateFile, useDefaultKeyGenFunc())
	} else {
		filePV = privValidator
	}

	testPrivValidatorKey, _ := cmtjson.MarshalIndent(filePV.Key, "", "  ")
	cmtos.MustWriteFile(privValKeyFile, []byte(testPrivValidatorKey), 0o644)

	// We always reset priv validator state to 0-height
	cmtos.MustWriteFile(privValStateFile, []byte(testPrivValidatorState), 0o644)

	return filePV
}

func useDefaultKeyGenFunc() func() (crypto.PrivKey, error) {
	return func() (crypto.PrivKey, error) {
		return ed25519.GenPrivKey(), nil
	}
}

// assertStartNodesMultiplex configures a nodes multiplex *randomly* and starts
// individual nodes in a separate goroutine per network.
func assertStartNodesMultiplex(t testing.TB, numChains int, customLogger cmtlog.Logger) (
	*config.Config,
	mx.MultiplexMap[*node.Node],
	*mx.Reactor,
) {
	_, globalCfg := ResetTestMultiplexNode(t, numChains)

	// Forces multi-test allowance, disables GRPC
	globalCfg.Instrumentation.Namespace = "cometbft:" + t.Name()
	globalCfg.GRPC.ListenAddress = ""            // disabled GRPC
	globalCfg.GRPC.Privileged.ListenAddress = "" // disabled GRPC

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainId, _ := range globalCfg.ChainSeeds {
		globalCfg.ChainSeeds[chainId] = ""
	}

	if customLogger == nil {
		customLogger = cmtlog.NewNopLogger()
	}

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	testMultiplex, testReactor, err := mx.NewNodesMultiplex(
		context.Background(),
		globalCfg,
		customLogger,
	)
	require.NoError(t, err, "should create node instance")
	require.NotNil(t, testMultiplex, "should return a multiplex map with a node")
	require.Len(t, testMultiplex, numChains, fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	// Test that we have all the required networks
	for _, testChainId := range testReactor.GetNetworks() {
		require.Contains(t, testMultiplex, testChainId)
		require.NotNil(t, testMultiplex[testChainId])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainId].GetInstance().(*node.Node)
		userAddress, err := testReactor.GetChainRegistry().GetAddress(testChainId)
		require.NoError(t, err, "should find user address by ChainID")

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(t, nodeInstance, globalCfg, userAddress, testChainId)

		// Verify that we can start the node correctly
		go func(cn *node.Node) {
			defer wg.Done()
			// t.Logf("Starting new node: %s", cn.GenesisDoc().ChainID)
			// t.Logf("Using listen addr: p2p:%s - rpc:%s", cn.Config().P2P.ListenAddress, cn.Config().RPC.ListenAddress)
			err := cn.Start()
			require.NoError(t, err)
		}(nodeInstance)
	}

	// Wait for all nodes to be up and running
	//t.Logf("Waiting for %d nodes to be up and running.", len(testReactor.GetNetworks()))
	wg.Wait()

	return globalCfg, testMultiplex, testReactor
}

func usePrivValidatorFromFiles(
	t testing.TB,
	n *node.Node,
	conf *config.Config,
	userAddress string,
	chainId string,
) {
	t.Helper()

	userConfDir := filepath.Join(conf.RootDir, config.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(conf.RootDir, config.DefaultDataDir, userAddress)

	privValKeyDir := filepath.Join(userConfDir, chainId)
	privValStateDir := filepath.Join(userDataDir, chainId)

	privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(conf.PrivValidatorKeyFile()))
	privValStateFile := filepath.Join(privValStateDir, filepath.Base(conf.PrivValidatorStateFile()))

	// Reload the priv validator from files. This overwrites the PrivValidator
	// so that it uses the default privval or a generated privval.
	newPV, err := privval.LoadOrGenFilePV(privValKeyFile, privValStateFile, useDefaultKeyGenFunc())
	require.NoError(t, err)
	// t.Logf("Using priv validator from files: %s\n", newPV.GetAddress())

	n.SetPrivValidator(newPV)

	consensusReactor := n.Switch().Reactor("CONSENSUS").(*cs.Reactor)
	consensusReactor.SetPrivValidator(newPV)
}

// CAUTION: this is not a GenesisDocSet, but just a GenesisDoc and is used
// to test the multiplex fallback to a legacy node implementation.
var testLegacyGenesisDocFmt = `{
	"genesis_time": "2018-10-10T08:20:13.695936996Z",
	"chain_id": "%s",
	"initial_height": "1",
	"consensus_params": {
		"block": {
			"max_bytes": "22020096",
			"max_gas": "-1",
			"time_iota_ms": "10"
		},
		"synchrony": {
			"message_delay": "500000000",
			"precision": "10000000"
		},
		"evidence": {
			"max_age_num_blocks": "100000",
			"max_age_duration": "172800000000000",
			"max_bytes": "1048576"
		},
		"validator": {
			"pub_key_types": [
				"ed25519"
			]
		},
		"abci": {
			"vote_extensions_enable_height": "0"
		},
		"version": {},
		"feature": {
			"vote_extensions_enable_height": "0",
			"pbts_enable_height": "1"
		}
	},
	"validators": [
	  ` + testDefaultGenesisValidator + `
	],
	"app_hash": ""
}`

var testGenesisValidatorPubKey = "AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="
var testDefaultGenesisValidator = `{
	"pub_key": {
		"type": "tendermint/PubKeyEd25519",
		"value":"` + testGenesisValidatorPubKey + `"
	},
	"power": "10",
	"name": ""
}`

// This produces a GenesisDocSet instance with exactly one chain.
var testGenesisDocWithValidatorsFmt = `{
	"genesis_time": "2018-10-10T08:20:13.695936996Z",
	"chain_id": "%s",
	"initial_height": "1",
	"consensus_params": {
		"block": {
			"max_bytes": "22020096",
			"max_gas": "-1",
			"time_iota_ms": "10"
		},
		"synchrony": {
			"message_delay": "500000000",
			"precision": "10000000"
		},
		"evidence": {
			"max_age_num_blocks": "100000",
			"max_age_duration": "172800000000000",
			"max_bytes": "1048576"
		},
		"validator": {
			"pub_key_types": [
				"ed25519"
			]
		},
		"abci": {
			"vote_extensions_enable_height": "0"
		},
		"version": {},
		"feature": {
			"vote_extensions_enable_height": "0",
			"pbts_enable_height": "1"
		}
	},
	"validators": [
		%s
	],
	"app_hash": ""
}`

var testValidatorFmt = `{
	"pub_key": {
		"type": "tendermint/PubKeyEd25519",
		"value":"%s"
	},
	"power": "%d",
	"name": ""
}`

var testPrivValidatorKeyFmt = `{
  "address": "%s",
  "pub_key": {
    "type": "tendermint/PubKeyEd25519",
    "value": "%s"
  },
  "priv_key": {
    "type": "tendermint/PrivKeyEd25519",
    "value": "%s"
  }
}`

var testPrivValidatorState = `{
  "height": "0",
  "round": 0,
  "step": 0
}`
