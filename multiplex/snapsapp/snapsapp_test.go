package snapsapp_test

import (
	"os"
	"testing"

	dbm "github.com/cometbft/cometbft-db"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"

	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/multiplex/snapsapp"
)

type (
	SnapsAppSuite struct {
		snapsApp    *snapsapp.SnapsApp
		chainStores mx.MultiplexChainStore
		logger      cmtlog.Logger
	}

	SnapshotsConfig struct {
		blocks             uint64
		snapshotInterval   uint64
		snapshotKeepRecent uint32
	}
)

const (
	exampleChainID = "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB"
)

func NewSnapsAppSuite(t *testing.T, opts ...func(*snapsapp.SnapsApp)) *SnapsAppSuite {
	t.Helper()

	rootDir, conf, chainRegistry, multiplexFS, multiplexStore := prepareMultiplex(t)
	defer os.RemoveAll(rootDir)

	logger := cmtlog.NewNopLogger()
	app := snapsapp.NewSnapsApplication(
		conf,
		chainRegistry,
		multiplexFS,
		multiplexStore,
		logger,
	)

	return &SnapsAppSuite{
		snapsApp:    app,
		chainStores: multiplexStore,
		logger:      logger,
	}
}

func prepareMultiplex(t *testing.T) (
	string,
	*config.Config,
	mx.ChainRegistry,
	mx.MultiplexFS,
	mx.MultiplexChainStore,
) {
	t.Helper()

	rootDir, err := os.MkdirTemp("", t.Name())
	if err != nil {
		panic(err)
	}

	conf := config.TestConfig()
	conf.BaseConfig = config.MultiplexTestBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			exampleChainID,
		}},
	)
	conf.SetRoot(rootDir)

	conf.SnapshotOptions = map[config.ReplicationStrategy]config.SnapshotOptions{}
	conf.SnapshotOptions[config.NewReplicationStrategy("Network")] = config.NewSnapshotOptions(1, 1000, 3)
	conf.SnapshotOptions[config.NewReplicationStrategy("History")] = config.NewSnapshotOptions(2, 2000, 3)

	chainRegistry, err := mx.NewChainRegistry(&conf.MultiplexConfig)
	require.NoError(t, err, "should create multiplex chain registry")

	multiplexFS, err := mx.NewMultiplexFS(conf)
	require.NoError(t, err, "should create multiplex filesystem paths")

	multiplexDB := mx.MultiplexDB{}
	for _, chainId := range chainRegistry.GetChains() {
		dbDir, err := os.MkdirTemp("", "test-mx-db-"+chainId)
		require.NoError(t, err, "should create custom database directory")

		db, err := dbm.NewDB(chainId, dbm.BackendType("memdb"), dbDir)
		require.NoError(t, err, "should create databases for chain")

		multiplexDB[chainId] = &mx.ChainDB{
			ChainID: chainId,
			DB:      db,
		}
	}

	multiplexStore := mx.NewMultiplexChainStore(
		multiplexDB,
		sm.StoreOptions{
			DiscardABCIResponses: false,
			DBKeyLayout:          "v2",
		},
		cmtlog.NewNopLogger(),
	)
	require.Len(t, multiplexStore, 1)

	return rootDir, conf, chainRegistry, multiplexFS, multiplexStore
}

func makeState(
	t *testing.T,
	chainId string,
	setHeight int64,
) (sm.State, []byte) {
	t.Helper()

	valPubKey := ed25519.GenPrivKey().PubKey()
	fakeAppHash := []byte{1, 2, 3}

	state, err := sm.MakeGenesisState(&types.GenesisDoc{
		GenesisTime:   cmttime.Now(),
		ChainID:       chainId,
		InitialHeight: 1000,
		Validators: []types.GenesisValidator{{
			Address: valPubKey.Address(),
			PubKey:  valPubKey,
			Power:   10,
			Name:    "myval",
		}},
		ConsensusParams: types.DefaultConsensusParams(),
		AppHash:         fakeAppHash,
		AppState:        []byte(`{"account_owner":"Alice"}`),
	})
	require.NoError(t, err, "should not error creating state machine")

	state.LastBlockHeight = setHeight
	state.LastBlockID = types.BlockID{}
	state.LastBlockTime = cmttime.Now()
	state.LastValidators = state.Validators

	return state, state.AppHash
}
