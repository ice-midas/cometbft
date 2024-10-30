package multiplex_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/node"
	sm "github.com/cometbft/cometbft/state"
	bs "github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

// multiplexGenesisDocProviderFunc mocks a GenesisDocSet provider helper
// by injecting ChainID values in genesis docs.
// CAUTION this should only be done for testing.
func mockMultiplexGenesisDocProviderFunc(
	conf *config.MultiplexConfig,
	numChains int,
) node.GenesisDocProvider {
	genesisDocSet := randomGenesisDocSet(numChains)

	// Requires synchrony between passed MultiplexConfig and numChains
	index := 0
	for _, chainIds := range conf.UserChains {
		for _, chainId := range chainIds {
			// CAUTION intentionally malleating GenesisDoc
			genesisDocSet[index].ChainID = chainId
			index++
		}
	}

	return func() (node.IChecksummedGenesisDoc, error) {
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs:    genesisDocSet,
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

func TestMultiplexReactorInitMultiplexStatesEmptyState(t *testing.T) {
	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexState(t, numChains, mx.KEY_DB_SM) // Uses database/state
	defer os.RemoveAll(rootDir)

	// InitMultiplexStates() uses `ValidateGenesisDocChecksum()` which reads
	// a genesisDocHashKey from the database. This following loop sets up a
	// mock environment, where odd chain indexes *do not* have a hash in db
	// and where even chain indexes *do* have a *valid* hash in db.
	// This permits to test the Checksum validation feature.
	for index, chainId := range reactor.GetNetworks() {
		// odd indexes do NOT have genesisDocHashKey set
		// this means that InitMultiplexStates will set it
		if index%2 != 0 {
			continue
		}

		databaseProvider := reactor.GetInstanceProvider(mx.KEY_DB_SM)
		assert.NotNil(t, databaseProvider, "should return multiplex map of database instances")

		stateDb := databaseProvider(chainId).(*mx.ChainDB)
		assert.NotNil(t, stateDb, "should return valid ChainDB per ChainID")

		// even indexes do have genesisDocHashKey set
		// it must match the genesisDoc`s SHA256 hash
		genesisDocProvider := reactor.GetGenesisProvider()
		genesisDoc := genesisDocProvider(chainId)
		genesisDocJSON, err := cmtjson.Marshal(genesisDoc)
		genesisDocHash := tmhash.Sum(genesisDocJSON)
		err = stateDb.DB.SetSync(genesisDocHashKey, genesisDocHash)
		assert.NoError(t, err)
	}

	// Execute the method being tested
	err := reactor.InitMultiplexStates()
	assert.NoError(t, err, "should not error given empty state in database")

	statesProvider := reactor.GetInstanceProvider(mx.KEY_STATE)
	assert.NotNil(t, statesProvider, "should not error getting states provider")

	// Do we have all state instances, with correct ChainID?
	for _, chainId := range reactor.GetNetworks() {
		chainState := statesProvider(chainId).(sm.State)
		assert.NotNil(t, chainState, "state instance per chain must not be nil")

		// And validate the ChainID
		assert.Equal(t, chainId, chainState.ChainID)

		// Test that we also have a genesisDocHashKey filled now
		databaseProvider := reactor.GetInstanceProvider(mx.KEY_DB_SM)
		assert.NotNil(t, databaseProvider, "should return multiplex map of database instances")
		stateDb := databaseProvider(chainId).(*mx.ChainDB)
		assert.NotNil(t, stateDb, "should return valid ChainDB per ChainID")

		genesisDocHash, err := stateDb.DB.Get(genesisDocHashKey)
		assert.NoError(t, err)
		assert.NotEmpty(t, genesisDocHash)
		assert.Len(t, genesisDocHash, tmhash.Size)
	}
}

func TestMultiplexReactorInitMultiplexStatesFilledState(t *testing.T) {
	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexState(t, numChains, mx.KEY_DB_SM) // Uses database/state
	defer os.RemoveAll(rootDir)

	genesisDocProvider := reactor.GetGenesisProvider()
	databaseProvider := reactor.GetInstanceProvider(mx.KEY_DB_SM)
	require.NotNil(t, databaseProvider, "should return multiplex map of database instances")

	testChainBlockHeight := int64(123)

	// Pre-populate the State store with some testable data
	for _, chainId := range reactor.GetNetworks() {
		genesisDoc := genesisDocProvider(chainId)
		stateDb := databaseProvider(chainId).(*mx.ChainDB)
		require.NotNil(t, stateDb, "should return valid ChainDB per ChainID")

		customState, err := sm.MakeGenesisState(genesisDoc)
		require.NoError(t, err, "should create state from genesis doc")

		// Validators are required for "sm.State" post block height 1
		validators := make([]*types.Validator, len(genesisDoc.Validators))
		for i, genDocValidator := range genesisDoc.Validators {
			validators[i] = types.NewValidator(genDocValidator.PubKey, genDocValidator.Power)
		}

		customState.Validators = types.NewValidatorSet(validators)
		customState.LastValidators = customState.Validators
		customState.NextValidators = customState.Validators

		// mutate state with testable data
		customState.LastBlockHeight = testChainBlockHeight // mutating Height
		customState.LastBlockID = types.BlockID{}
		customState.AppHash = tmhash.Sum([]byte(chainId)) // mutating AppHash

		// CAUTION: we inject a custom State here
		err = stateDb.DB.SetSync(stateKey, customState.Bytes())
		require.NoError(t, err, "should update state instance in database")
	}

	// Execute the method being tested
	err := reactor.InitMultiplexStates()
	assert.NoError(t, err, "should not error given filled state in database")

	statesProvider := reactor.GetInstanceProvider(mx.KEY_STATE)
	assert.NotNil(t, statesProvider, "should not error getting states provider")

	// Do we have all state instances, with correct ChainID?
	for _, chainId := range reactor.GetNetworks() {
		chainState := statesProvider(chainId).(sm.State)
		assert.NotNil(t, chainState, "state instance per chain must not be nil")

		// And validate the loaded state instance contains our
		// mutated values with correct correspondance.
		assert.Equal(t, chainId, chainState.ChainID)
		assert.Equal(t, testChainBlockHeight, chainState.LastBlockHeight) // mutated height

		expectedSha256 := tmhash.Sum([]byte(chainId))
		assert.Equal(t, expectedSha256, chainState.AppHash) // mutated AppHash
	}
}

func TestMultiplexReactorInitMultiplexBlockStores(t *testing.T) {
	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexState(t, numChains, mx.KEY_DB_BS) // Uses database/blockStore
	defer os.RemoveAll(rootDir)

	databaseProvider := reactor.GetInstanceProvider(mx.KEY_DB_BS)
	require.NotNil(t, databaseProvider, "should return multiplex map of database instances")

	// Execute the method being tested
	err := reactor.InitMultiplexBlockStores()
	assert.NoError(t, err, "should not error given empty state in database")

	blockStoresProvider := reactor.GetInstanceProvider(mx.KEY_STORE_BLOCK)
	assert.NotNil(t, blockStoresProvider, "should not error getting states provider")

	// Do we have all blockStore databases?
	for _, chainId := range reactor.GetNetworks() {
		// Type-assertion makes sure we have correct type
		chainBlockStore := blockStoresProvider(chainId).(*bs.BlockStore)
		assert.NotNil(t, chainBlockStore, "blockStore instance per chain must not be nil")
		assert.IsType(t, &bs.BlockStore{}, chainBlockStore)
	}
}

// CAUTION: the GenesisDocProvider is maleated to contain correct ChainIDs
// CAUTION: the MultiplexConfig is entirely and *not synchronized* with genesis docs.
func ResetTestMultiplexState(t testing.TB, numChains int, dbInstanceKey string) (string, *config.Config, *mx.Reactor) {
	t.Helper()

	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, numChains)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&nodeCfg.MultiplexConfig, numChains)

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(t, nodeCfg, mockGenesisProvider)

	// Prepares the database for each network
	for index, chainId := range reactor.GetNetworks() {
		// We create one state database instance per chain
		dbName := "chaindb-state-" + strconv.Itoa(index)
		stateDb, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
		require.NoError(t, err)

		// This instance is retrieved in InitMultiplexStates()
		reactor.RegisterInstance(dbInstanceKey, chainId, &mx.ChainDB{
			ChainID: chainId,
			DB:      stateDb,
		}) // "database/state"
	}

	return rootDir, nodeCfg, reactor
}
