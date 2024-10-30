package multiplex_test

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"

	mx "github.com/cometbft/cometbft/multiplex"
)

func makeRandomNodeKey() *p2p.NodeKey {
	priv := ed25519.GenPrivKey()
	return &p2p.NodeKey{PrivKey: priv}
}

// mockGenesisDocSetProviderFunc mocks a GenesisDocSet provider helper.
func mockGenesisDocSetProviderFunc() node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs:    randomGenesisDocSet(3),
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

// mockErrorGenesisDocSetProviderFunc mocks a provider helper that errors.
func mockErrorGenesisDocSetProviderFunc() node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		return nil, errors.New("testError")
	}
}

func TestMultiplexReactorNewReactor(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeKey := makeRandomNodeKey()
	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5) // 5 distinct networks

	chainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig)
	require.NoError(t, err, "should create chain registry instance")

	// ----------------
	// Errors
	// Should panic given an error when executing the GenesisDocProvider.
	assert.Panics(t, func() {
		mx.NewReactor(
			nodeKey,
			nodeCfg,
			cmtlog.NewNopLogger(),
			chainRegistry,
			mockErrorGenesisDocSetProviderFunc(), // error!
		)
	}, "should panic given an error with GenesisDocProvider")

	// ----------------
	// Successes

	// Test a successful configuration of a Reactor
	reactor := mx.NewReactor(
		nodeKey,
		nodeCfg,
		cmtlog.NewNopLogger(),
		chainRegistry,
		mockGenesisDocSetProviderFunc(),
	)

	// NewReactor may not return nil
	assert.NotNil(t, reactor)

	// Networks must be ordered
	// ChainRegistry.GetChains() is tested to produce an ordered slice.
	assert.NotEmpty(t, reactor.GetNetworks())
	for i, testChainId := range chainRegistry.GetChains() {
		actualChainId := reactor.GetNetworks()[i]
		assert.Equal(t, testChainId, actualChainId)
	}

	// NewReactor must initialize providers
	assert.NotNil(t, reactor.GetGenesisProvider())
	assert.NotNil(t, reactor.GetServicesProvider())
	assert.NotNil(t, reactor.GetMultiplexProvider())
}

func TestMultiplexReactorRegisterService(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5) // 5 distinct networks

	// Create a test reactor
	reactor := makeTestReactor(t, nodeCfg)

	// Create services per chain
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainId := range chainIds {
			// Test registering a valid service
			eventBus := types.NewEventBus()
			reactor.RegisterService(mx.KEY_EVENTBUS, chainId, eventBus)
		}
	}

	// And retrieve to assert
	servicesProvider := reactor.GetServicesProvider()
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainId := range chainIds {
			// Type-assertion to cast back to actual service structure
			eventBus := servicesProvider(mx.KEY_EVENTBUS, chainId).(*types.EventBus)

			assert.NotNil(t, eventBus, "service provider should return service instance")
			assert.IsType(t, &types.EventBus{}, eventBus)
		}
	}

	// ------------------------------------------------
	// RESET reactor
	otherReactor := makeTestReactor(t, nodeCfg)

	// Following tests the mutex for services and makes sure that the service
	// provider is thread-safe and retrieval of services is always possible.
	var wg sync.WaitGroup
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainId := range chainIds {
			wg.Add(1)
			go func(concurrentChainId string) {
				// Test registering a valid service in parallel goroutine
				eventBus := types.NewEventBus()
				otherReactor.RegisterService(mx.KEY_EVENTBUS, concurrentChainId, eventBus)

				wg.Done()
			}(chainId)
		}
	}

	wg.Wait()

	// And retrieve to assert
	otherServicesProvider := otherReactor.GetServicesProvider()
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainId := range chainIds {
			// Type-assertion to cast back to actual service structure
			eventBus := otherServicesProvider(mx.KEY_EVENTBUS, chainId).(*types.EventBus)

			assert.NotNil(t, eventBus, "service provider should return service instance")
			assert.IsType(t, &types.EventBus{}, eventBus)
		}
	}
}

func TestMultiplexReactorRegisterInstance(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5) // 5 distinct networks

	// Create a test reactor
	reactor := makeTestReactor(t, nodeCfg)

	testDbKey := []byte(`testChainId`)

	// Create instances per chain
	// Using ORDERED networks because of ports overwrite content test
	for index, chainId := range reactor.GetNetworks() {
		// 1. We create a mutated config per chain
		perChainCfg := mx.NewConfigOverwrite(
			nodeCfg,
			reactor.GetChainRegistry(),
			chainId,
		)

		// 2. We create a database instance per chain
		dbName := "chaindb-" + strconv.Itoa(index)
		perChainDb, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
		require.NoError(t, err)
		// .. and add some data to it
		err = perChainDb.SetSync(testDbKey, []byte(chainId))

		// Test registering a valid instance
		reactor.RegisterInstance(mx.KEY_CONFIG, chainId, perChainCfg) // "config"
		reactor.RegisterInstance(mx.KEY_DB_SM, chainId, &mx.ChainDB{
			ChainID: chainId,
			DB:      perChainDb,
		}) // "database/state"
	}

	// And retrieve to assert
	configProvider := reactor.GetInstanceProvider(mx.KEY_CONFIG)
	assert.NotNil(t, configProvider, "should return multiplex map of config instances")

	databaseProvider := reactor.GetInstanceProvider(mx.KEY_DB_SM)
	assert.NotNil(t, databaseProvider, "should return multiplex map of database instances")

	// Using ORDERED networks because of ports overwrite content test
	for index, chainId := range reactor.GetNetworks() {
		// 1. Type-assertion to cast back to actual instance
		perChainCfg := configProvider(chainId).(*config.Config)

		assert.NotNil(t, perChainCfg, "instance provider should return instance")
		assert.IsType(t, &config.Config{}, perChainCfg)

		// Test that the content is from the right config object
		expectedP2PPort := int(nodeCfg.P2PStartPort) + index
		expectedRPCPort := int(nodeCfg.RPCStartPort) + index
		assert.Contains(t, perChainCfg.P2P.ListenAddress, strconv.Itoa(expectedP2PPort))
		assert.Contains(t, perChainCfg.RPC.ListenAddress, strconv.Itoa(expectedRPCPort))

		// 2. Also do some asserts about the DB instance stored
		perChainDb := databaseProvider(chainId).(*mx.ChainDB)

		assert.NotNil(t, perChainDb, "instance provide should return instance")
		assert.IsType(t, &mx.ChainDB{}, perChainDb)

		actualChainId, err := perChainDb.DB.Get(testDbKey)
		assert.NoError(t, err, "should retrieve key from database")
		assert.Equal(t, []byte(chainId), actualChainId)
	}

	// ------------------------------------------------
	// RESET reactor
	otherReactor := makeTestReactor(t, nodeCfg)

	// Following tests the mutex for services and makes sure that the service
	// provider is thread-safe and retrieval of services is always possible.
	var wg sync.WaitGroup
	for index, chainId := range otherReactor.GetNetworks() {
		wg.Add(1)
		go func(idx int, concurrentChainId string) {
			// 1. We create a mutated config per chain
			perChainCfg := mx.NewConfigOverwrite(
				nodeCfg,
				otherReactor.GetChainRegistry(),
				chainId,
			)

			// 2. We create a database instance per chain
			dbName := "chaindb-" + strconv.Itoa(idx)
			perChainDb, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
			require.NoError(t, err)
			// .. and add some data to it
			err = perChainDb.SetSync(testDbKey, []byte(concurrentChainId))

			// Test registering a valid instance in parallel goroutines
			otherReactor.RegisterInstance(mx.KEY_CONFIG, concurrentChainId, perChainCfg) // "config"
			otherReactor.RegisterInstance(mx.KEY_DB_SM, concurrentChainId, &mx.ChainDB{
				ChainID: concurrentChainId,
				DB:      perChainDb,
			}) // "database/state"

			wg.Done()
		}(index, chainId)
	}

	wg.Wait()

	// And retrieve to assert
	otherConfigProvider := reactor.GetInstanceProvider(mx.KEY_CONFIG)
	assert.NotNil(t, otherConfigProvider, "should return multiplex map of config instances")

	otherDatabaseProvider := reactor.GetInstanceProvider(mx.KEY_DB_SM)
	assert.NotNil(t, otherDatabaseProvider, "should return multiplex map of database instances")
	for index, chainId := range otherReactor.GetNetworks() {
		// 1. Type-assertion to cast back to actual instance
		perChainCfg := otherConfigProvider(chainId).(*config.Config)

		assert.NotNil(t, perChainCfg, "instance provider should return instance")
		assert.IsType(t, &config.Config{}, perChainCfg)

		// Test that the content is from the right config object
		expectedP2PPort := int(nodeCfg.P2PStartPort) + index
		expectedRPCPort := int(nodeCfg.RPCStartPort) + index
		assert.Contains(t, perChainCfg.P2P.ListenAddress, strconv.Itoa(expectedP2PPort))
		assert.Contains(t, perChainCfg.RPC.ListenAddress, strconv.Itoa(expectedRPCPort))

		// 2. Also do some asserts about the DB instance stored
		perChainDb := otherDatabaseProvider(chainId).(*mx.ChainDB)

		assert.NotNil(t, perChainDb, "instance provide should return instance")
		assert.IsType(t, &mx.ChainDB{}, perChainDb)

		actualChainId, err := perChainDb.DB.Get(testDbKey)
		assert.NoError(t, err, "should retrieve key from database")
		assert.Equal(t, []byte(chainId), actualChainId)
	}
}

// Do not use this in TestMultiplexReactorNewReactor
func makeTestReactor(t testing.TB, nodeCfg *config.Config) *mx.Reactor {
	t.Helper()

	return makeTestReactorWithGenesisDocProvider(t, nodeCfg, mockGenesisDocSetProviderFunc())
}

func makeTestReactorWithGenesisDocProvider(
	t testing.TB,
	nodeCfg *config.Config,
	genDocProvider node.GenesisDocProvider,
) *mx.Reactor {

	nodeKey := makeRandomNodeKey()

	chainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig)
	require.NoError(t, err, "should create chain registry instance")

	return mx.NewReactor(
		nodeKey,
		nodeCfg,
		cmtlog.NewNopLogger(),
		chainRegistry,
		genDocProvider,
	)
}
