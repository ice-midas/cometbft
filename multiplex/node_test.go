package multiplex

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmttest "github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/libs/log"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
	cmtnode "github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
)

// It is primordial that this test always uses `cmtnode` to be able to evaluate
// the *default node behaviour* without multiplex ; and it should also be first.
func TestMultiplexNodeLegacyGenesis(t *testing.T) {
	// cmtnode.setup defines private constants that are overwritten
	// in cmtmx.setup and therefor this test must use the legacy key
	legacyGenesisDocHashKey := []byte("genesisDocHash")

	config := cmttest.ResetTestRoot("mx_node_legacy_genesis")
	defer os.RemoveAll(config.RootDir)

	// Use goleveldb so we can reuse the same db for the second NewNode()
	config.DBBackend = string(dbm.GoLevelDBBackend)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	n, err := cmtnode.NewNode(
		context.Background(),
		config,
		privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile()),
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		cmtnode.DefaultGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)

	// Start and stop to close the db for later reading
	err = n.Start()
	require.NoError(t, err)

	err = n.Stop()
	require.NoError(t, err)

	// Ensure the genesis doc hash is saved to db
	stateDB, err := cfg.DefaultDBProvider(&cfg.DBContext{ID: "state", Config: config})
	require.NoError(t, err)

	genDocHash, err := stateDB.Get(legacyGenesisDocHashKey)
	require.NoError(t, err)
	assert.NotNil(t, genDocHash, "genesis doc hash should be saved in db under 'genesisDoc'")
	assert.Len(t, genDocHash, tmhash.Size)

	err = stateDB.Close()
	require.NoError(t, err)
}

func TestMultiplexNodeSingularReplicationFallbackWithEmptyScopes(t *testing.T) {
	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_new_node_fallback_empty_scopes",
		"",
		map[string][]string{},
	)
	defer os.RemoveAll(config.RootDir)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	r, err := NewMultiplexNode(
		context.Background(),
		config,
		privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile()),
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		PluralUserGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, 1, len(r.Nodes), "the registry should contain exactly one node")
	assert.Contains(t, r.Nodes, "") // empty key for singular mode
}

func TestMultiplexNodePluralReplicationConfig(t *testing.T) {
	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_new_node_plural_replication_config",
		"",
		map[string][]string{
			// mind changing mxtest.TestScopeHash if you make a change here.
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	r, err := NewMultiplexNode(
		context.Background(),
		config,
		privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile()),
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		PluralUserGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, 1, len(r.Nodes), "the registry should contain exactly one node")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode
	assert.Contains(t, r.Nodes, mxtest.TestScopeHash)
}

func TestMultiplexNodePluralReplicationConfigWithManyNodes(t *testing.T) {
	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_new_node_plural_replication_config_with_many_nodes",
		"",
		map[string][]string{
			// mind changing mxtest.TestScopeHash if you make a change here.
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF190888BE0F48DE88927C3F49215B96548273AF": {"Scoping"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Private"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"Globals"},
			"EB6E5F1BFF0B9F74C2731090ADE7B5C78EEEA74F": {"Default"},
			"5DD7B1B86EDABBC1C077CB9D846805F7FA10B0DD": {"Spaced scope"},
			"08A6F3BFF2C4513A3D5BD057A5DF3B274676750F": {"Default"},
			"76D0E67F5CD8EEFFA238E5CF27969AFB6C1AC73B": {"Default"},
			"7A435E5D913D8A4960658F51B7E260BA512F6CAD": {"Default"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	numReplicatedChains := 9

	r, err := NewMultiplexNode(
		context.Background(),
		config,
		privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile()),
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		PluralUserGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, numReplicatedChains, len(r.Nodes), "the registry should contain correct number of nodes")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode
	assert.Contains(t, r.Nodes, mxtest.TestScopeHash)
}

func TestMultiplexNodeDefaultMultiplexNode(t *testing.T) {

	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_default_multiplex_node_start_stop",
		"",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	// create & start node
	r, err := DefaultMultiplexNode(config, log.TestingLogger())
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, 1, len(r.Nodes), "the registry should contain exactly one node")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode
}

// XXX TestMultiplexNodeCreatesMultipleAddressBooks
// XXX TestMultiplexNodeCreatesMultipleWalFiles
// XXX TestMultiplexNodeCreatesMultipleWalFiles
