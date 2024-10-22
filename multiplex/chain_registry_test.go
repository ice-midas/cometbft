package multiplex_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/config"
	cmtos "github.com/cometbft/cometbft/internal/os"

	mx "github.com/cometbft/cometbft/multiplex"
)

func TestMultiplexChainRegistryLoadSeedsFromFile(t *testing.T) {
	// ----------------
	// Errors
	// create temporary file
	errfile, err := os.CreateTemp("", "errors.seeds.json")
	require.NoError(t, err)
	defer os.Remove(errfile.Name())

	// Should error if file doesn't exist
	_, err = mx.LoadSeedsFromFile("invalid.file")
	assert.Error(t, err, "should error given unknown file")

	failCases := [][]byte{
		[]byte(`1234`),
		[]byte(`{}`),
		[]byte(`{"abc": 1}`),
		[]byte(`{"def": null}`),
		[]byte(`{"": ""}`),
	}

	for _, failCaseSeedsBytes := range failCases {
		cmtos.WriteFile(errfile.Name(), failCaseSeedsBytes, 0644)

		// Should drop empty seed configurations
		seeds, _ := mx.LoadSeedsFromFile(errfile.Name())
		assert.Empty(t, seeds)
	}

	// ----------------
	// Successes
	// create temporary file
	tmpfile, err := os.CreateTemp("", "seeds.json")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	seedsBytes := []byte(`{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "seed1@127.0.0.1:30001,seed2@127.0.0.1:30002"
	}`)
	cmtos.WriteFile(tmpfile.Name(), seedsBytes, 0644)

	seeds, err := mx.LoadSeedsFromFile(tmpfile.Name())
	assert.NoError(t, err)
	assert.Len(t, seeds, 1) // one key (user address)

	seedsBytes = []byte(`{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "seed1@127.0.0.1:30001",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC": "seed2@127.0.0.1:30002",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC": "seed3@127.0.0.1:30003"
	}`)
	cmtos.WriteFile(tmpfile.Name(), seedsBytes, 0644)

	seeds, err = mx.LoadSeedsFromFile(tmpfile.Name())
	assert.NoError(t, err)
	assert.Len(t, seeds, 3) // three key (user address)
	assert.Contains(t, seeds, "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB")
}

func TestMultiplexChainRegistryLoadChainsFromGenesisFile(t *testing.T) {
	// ----------------
	// Errors
	// create temporary file
	errfile, err := os.CreateTemp("", "errors.genesis.json")
	require.NoError(t, err)
	defer os.Remove(errfile.Name())

	// Should error if file doesn't exist
	_, err = mx.LoadChainsFromGenesisFile("invalid.file")
	assert.Error(t, err, "should error given unknown file")

	// ----------------
	// Successes
	// create temporary file
	tmpfile, err := os.CreateTemp("", "seeds.json")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// save genesis doc set in temp file
	numChains := 3
	genesisDocSet := randomGenesisDocSet(numChains)
	require.Len(t, genesisDocSet, numChains)
	err = genesisDocSet.SaveAs(tmpfile.Name())
	require.NoError(t, err)

	userChains, err := mx.LoadChainsFromGenesisFile(tmpfile.Name())
	assert.NoError(t, err, "should not error given valid GenesisDocSet")

	// In this test, we use the validator address as the chain owner by
	// force-including the validator address in the ChainID.
	// see also: randomGenesisDocSet()
	for _, testGenesisDoc := range genesisDocSet {
		expectedOwner := testGenesisDoc.Validators[0].Address.String()
		expectedChainID := formalizeChainID(testGenesisDoc.ChainID)

		// Must map ChainIDs to user addresses
		assert.Contains(t, userChains, expectedOwner)

		// Must contain correct ChainID
		assert.Contains(t, userChains[expectedOwner], expectedChainID)
	}
}

func TestMultiplexChainRegistryNewChainRegistry(t *testing.T) {
	// ----------------
	// Errors
	// Should not do anything given "Disabled" strategy
	stopConf := config.EmptyMultiplexConfig() // contains ReplicationStrategy{"Disabled"}
	stopRegistry, err := mx.NewChainRegistry(&stopConf)
	assert.NoError(t, err, "should not error given disabled multiplex configuration")
	assert.Empty(t, stopRegistry.GetChains())

	// Should error given empty replicated chains but "Network" strategy
	misConf := config.MultiplexTestBaseConfig( // contains ReplicationStrategy("Network")
		map[string]*config.StateSyncConfig{},
		map[string]string{
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "id@host:port",
		},
		map[string][]string{},
	)
	_, err = mx.NewChainRegistry(&misConf.MultiplexConfig)
	assert.Error(t, err, "should error given no replicated chains")

	// ----------------
	// Successes
	minimalConf := config.MultiplexTestBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		}},
	)
	minimalRegistry, err := mx.NewChainRegistry(&minimalConf.MultiplexConfig)
	assert.NoError(t, err, "should not error given valid minimal multiplex configuration")
	assert.Len(t, minimalRegistry.GetChains(), 1)

	exampleConf := config.MultiplexTestBaseConfig(
		map[string]*config.StateSyncConfig{
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": config.DefaultStateSyncConfig(),
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC": config.DefaultStateSyncConfig(),
			"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC": config.DefaultStateSyncConfig(),
		},
		map[string]string{
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "seed1@127.0.0.1:30001",
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC": "seed1@127.0.0.1:30002",
			"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC": "seed1@127.0.0.1:30003",
		},
		map[string][]string{
			// The order here doesn't matter, so we intentionally force a sorting operation.
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {
				"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
			},
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
			},
		},
	)
	chainRegistry, err := mx.NewChainRegistry(&exampleConf.MultiplexConfig)
	assert.NoError(t, err, "should not error given valid example multiplex configuration")
	assert.Len(t, chainRegistry.GetChains(), 3)

	// Should be ordered even though the above configuration intentionally passes unordered ChainIDs
	assert.Equal(t, chainRegistry.GetChains()[0], "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB")
	assert.Equal(t, chainRegistry.GetChains()[1], "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC")
	assert.Equal(t, chainRegistry.GetChains()[2], "mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC")

}
