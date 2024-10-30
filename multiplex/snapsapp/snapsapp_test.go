package snapsapp_test

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"

	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/multiplex/snapsapp"
)

type (
	SnapsAppSuite struct {
		snapsApp *snapsapp.SnapsApp
		reactor  *mx.Reactor
		logger   cmtlog.Logger
		rootDir  string
	}

	SnapshotsConfig struct {
		blocks             uint64
		snapshotInterval   uint64
		snapshotKeepRecent uint32
	}
)

const (
	baseExampleChainID = "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-"
)

// mockGenesisDocSetProviderFunc mocks a GenesisDocSet provider helper.
func mockGenesisDocSetProviderFunc(withChainID string) node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		// random validators, careful with this provider.
		valPubKey := ed25519.GenPrivKey().PubKey()
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs: mx.GenesisDocSet{
				types.GenesisDoc{
					GenesisTime:   cmttime.Now(),
					ChainID:       withChainID,
					InitialHeight: 1000,
					Validators: []types.GenesisValidator{{
						Address: valPubKey.Address(),
						PubKey:  valPubKey,
						Power:   10,
						Name:    "myval",
					}},
					ConsensusParams: types.DefaultConsensusParams(),
					AppHash:         []byte{1, 2, 3},
					AppState:        []byte(`{"account_owner":"Bob"}`),
				},
			},
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

func NewSnapsAppSuite(t *testing.T, opts ...func(*snapsapp.SnapsApp)) *SnapsAppSuite {
	t.Helper()

	rootDir, _, testReactor := prepareMultiplexReactor(t)

	logger := cmtlog.NewNopLogger()
	app := snapsapp.NewSnapsApplication(testReactor, config.NewSnapshotOptions(1, 1, 1), logger)

	return &SnapsAppSuite{
		snapsApp: app,
		reactor:  testReactor,
		logger:   logger,
		rootDir:  rootDir,
	}
}

func prepareMultiplexReactor(t *testing.T) (
	string,
	*config.Config,
	*mx.Reactor,
) {
	t.Helper()

	rootDir, err := os.MkdirTemp("", t.Name())
	if err != nil {
		panic(err)
	}

	// Create a custom chain id for each iteration (based on test name)
	// This should be random enough to produce non-repeating values
	testChainId := baseExampleChainID + strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte(t.Name()))[:8], // 8 bytes only
	))

	conf := config.TestConfig()
	conf.BaseConfig = config.MultiplexTestBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			testChainId,
		}},
	)
	conf.SetRoot(rootDir)

	conf.SnapshotOptions = map[config.ReplicationStrategy]config.SnapshotOptions{}
	conf.SnapshotOptions[config.NewReplicationStrategy("Network")] = config.NewSnapshotOptions(1, 1000, 3)
	conf.SnapshotOptions[config.NewReplicationStrategy("History")] = config.NewSnapshotOptions(2, 2000, 3)

	nodeKey := makeRandomNodeKey()
	testChainRegistry, err := mx.NewChainRegistry(&conf.MultiplexConfig)
	require.NoError(t, err, "should create chain registry instance")

	// Test Reactor implementation in multiplex package
	testReactor := mx.NewReactor(
		nodeKey,
		conf,
		cmtlog.NewNopLogger(),
		testChainRegistry,
		mockGenesisDocSetProviderFunc(testChainId),
	)

	err = testReactor.Start()
	require.NoError(t, err, "should not error starting multiplex reactor")

	return rootDir, conf, testReactor
}

func makeRandomNodeKey() *p2p.NodeKey {
	priv := ed25519.GenPrivKey()
	return &p2p.NodeKey{PrivKey: priv}
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
