package multiplex_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/config"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmtrand "github.com/cometbft/cometbft/internal/rand"

	mx "github.com/cometbft/cometbft/multiplex"
)

func TestMultiplexDBChainID(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-db-chain-id", 1000)
	defer os.RemoveAll(rootDir)

	assert.Equal(t, true, cmtos.FileExists(rootDir))

	for chainId, chainDb := range multiplexDb {
		assert.NotEmpty(t, chainId)
		assert.Equal(t, chainId, chainDb.ChainID)
		assert.NotNil(t, chainDb.DB)
	}
}

func TestMultiplexDBSimpleSetGet(t *testing.T) {
	numDatabases := 1000

	// Reset test fs
	rootDir, dbPtrs := ResetMultiDBTestRoot(t, "test-multi-db-set-get-key", numDatabases)
	defer os.RemoveAll(rootDir)

	// Mocks a key-value pair
	key := []byte(`key`)
	data := []byte(`value`)

	randomizer := cmtrand.NewRand()
	dbIdx := randomizer.Intn(numDatabases)
	err := dbPtrs[dbIdx].Set(key, data)
	require.NoError(t, err)

	actual, err := dbPtrs[dbIdx].Get(key)
	require.NoError(t, err)
	assert.Equal(t, data, actual)
}

func TestMultiplexDBParallelSetGet(t *testing.T) {
	numDatabases := 1000
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-db-parallel-set-get", numDatabases)
	defer os.RemoveAll(rootDir)

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	// Mocks a key-value pair
	key := []byte(`key`)
	data := []byte(`value`)

	// Use an array, not user-indexed here
	dbs := make([]*mx.ChainDB, numDatabases)
	i := 0
	for _, db := range multiplexDb {
		dbs[i] = db
		i++
	}

	t.Run("parallel", func(t *testing.T) {
		t.Parallel()

		dbIdx := randomizer.Intn(numDatabases)
		err := dbs[dbIdx].Set(key, data)
		assert.NoError(t, err)

		actual, err := dbs[dbIdx].Get(key)
		require.NoError(t, err)
		assert.Equal(t, data, actual)
	})
}

func TestMultiplexDBNewMultiplexDB(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	conf := config.TestConfig()

	// ----------------
	// Errors
	failCases := []string{
		"invalid-chain-id",
		"cosmoshub-4",
		"mx-chain-1234-5678",
		// invalid addresses
		"mx-chain-#000000000000000000000000000000000000000-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC4-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43AAB-1A63C0E60122F9BB",
		// invalid fingerprints
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-#000000000000000",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BBCC",
	}

	for _, failCaseChainId := range failCases {
		conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]*config.StateSyncConfig{}, map[string]string{}, map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {failCaseChainId},
		})
		conf.SetRoot(rootDir)

		_, err := mx.NewMultiplexDB(&mx.ChainDBContext{
			DBContext: config.DBContext{ID: "state", Config: conf},
		})
		assert.Error(t, err, "should forward error given invalid configuration")
	}

	// ----------------
	// Successes
	exampleChains := []string{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
	}

	conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]*config.StateSyncConfig{}, map[string]string{}, map[string][]string{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {exampleChains[0], exampleChains[1]},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {exampleChains[2]},
	})
	conf.SetRoot(rootDir)

	multiplex, err := mx.NewMultiplexDB(&mx.ChainDBContext{
		DBContext: config.DBContext{ID: "state", Config: conf},
	})

	assert.NoError(t, err, "should not error given valid configuration")
	assert.Len(t, multiplex, len(exampleChains), "should create correct number of databases")
	assert.Contains(t, multiplex, exampleChains[0])
	assert.Contains(t, multiplex, exampleChains[1])
	assert.Contains(t, multiplex, exampleChains[2])
	assert.Equal(t, exampleChains[0], multiplex[exampleChains[0]].ChainID)
	assert.Equal(t, exampleChains[1], multiplex[exampleChains[1]].ChainID)
	assert.Equal(t, exampleChains[2], multiplex[exampleChains[2]].ChainID)
	assert.NotNil(t, multiplex[exampleChains[0]].DB)
	assert.NotNil(t, multiplex[exampleChains[1]].DB)
	assert.NotNil(t, multiplex[exampleChains[2]].DB)
}

// ----------------------------------------------------------------------------
// Exported helpers

func ResetMultiDBTestRoot(
	t testing.TB,
	testName string,
	numDatabases int,
) (string, []dbm.DB) {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	// Open numDatabases database adapters
	dbPtrs, err := createTempMemDB(t, rootDir, numDatabases)
	require.NoError(t, err, "should create MemDB instances in temp directory")

	return rootDir, dbPtrs
}

func ResetMultiplexDBTestRoot(
	t testing.TB,
	testName string,
	numDatabases int,
) (string, mx.MultiplexDB) {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	// Open numDatabases database adapters
	dbPtrs, err := createTempMemDB(t, rootDir, numDatabases)
	require.NoError(t, err, "should create MemDB instances in temp directory")

	// Create a database multiplex
	multiplex := mx.MultiplexDB{}
	for i, db := range dbPtrs {
		chainId := "test-chain-" + strconv.Itoa(i)
		multiplex[chainId] = &mx.ChainDB{
			ChainID: chainId,
			DB:      db,
		}
	}

	return rootDir, multiplex
}

// ----------------------------------------------------------------------------

func createTempMemDB(
	t testing.TB,
	rootDir string,
	numDatabases int,
) ([]dbm.DB, error) {
	t.Helper()

	dbPtrs := make([]dbm.DB, numDatabases)
	dbType := dbm.BackendType("memdb")

	for i := 0; i < numDatabases; i++ {
		dbDir, err := os.MkdirTemp(rootDir, "cometbft-memdb-*")
		if err != nil {
			return dbPtrs, err
		}

		dbId := "db-" + strconv.Itoa(i)
		db, err := dbm.NewDB(dbId, dbType, dbDir)
		require.NoError(t, err, "should create new DB instance")

		dbPtrs[i] = db
		db.Close() // noop for memdb
	}

	return dbPtrs, nil
}
