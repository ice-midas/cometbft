package multiplex

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmttest "github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/libs/log"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	types "github.com/cometbft/cometbft/types"
)

func TestMultiplexNodeNewNodeGenesisHashMismatch(t *testing.T) {
	config := cmttest.ResetTestRoot("node_new_node_genesis_hash")
	defer os.RemoveAll(config.RootDir)

	// Use goleveldb so we can reuse the same db for the second NewNode()
	config.DBBackend = string(dbm.GoLevelDBBackend)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	n, err := node.NewNode(
		context.Background(),
		config,
		privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile()),
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		node.DefaultGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		node.DefaultMetricsProvider(config.Instrumentation),
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

	genDocHash, err := stateDB.Get(genesisDocHashKey)
	require.NoError(t, err)
	require.NotNil(t, genDocHash, "genesis doc hash should be saved in db")
	require.Len(t, genDocHash, tmhash.Size)

	err = stateDB.Close()
	require.NoError(t, err)

	// Modify the genesis file chain ID to get a different hash
	genBytes := cmtos.MustReadFile(config.GenesisFile())
	var genesisDocSet *GenesisDocSet
	var genesisDoc types.GenesisDoc
	genesisDocSet, err = GenesisDocSetFromJSON(genBytes)
	require.NoError(t, err)

	userGenDoc, ok, err := genesisDocSet.SearchGenesisDocByScope(mxtest.TestScopeHash)
	require.NoError(t, err)
	require.Equal(t, ok, true)

	genesisDoc = userGenDoc.GenesisDoc
	genesisDoc.ChainID = "different-chain-id"
	err = genesisDocSet.SaveAs(config.GenesisFile())
	require.NoError(t, err)

	_, err = node.NewNode(
		context.Background(),
		config,
		privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile()),
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		node.DefaultGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		node.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.Error(t, err, "NewNode should error when genesisDoc is changed")
	require.Equal(t, "genesis doc hash in db does not match loaded genesis doc", err.Error())
}
