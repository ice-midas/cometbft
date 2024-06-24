package multiplex

import (
	"encoding/base64"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfg "github.com/cometbft/cometbft/config"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
	"github.com/cometbft/cometbft/privval"
	types "github.com/cometbft/cometbft/types"
)

func TestMultiplexValidatorSetGenesisUsesCorrectValidators(t *testing.T) {

	testName := "mx_test_validator_set_genesis_uses_correct_validators"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	validatorsPubs, _ := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
		nil,
		0, // 0-startPortOverwrite (defaults to 30001)
	)

	expectedNumChains := 1
	expectedValidators := numValidators + 1 // priv validator

	require.Equal(t, expectedNumChains, len(r.Nodes))

	actualScopedNode := r.Nodes[mxtest.TestScopeHash]
	actualGenesisDoc := actualScopedNode.GenesisDoc()
	actualValidators := actualGenesisDoc.Validators

	actualPrivValPub, err := actualScopedNode.PrivValidator().GetPubKey()
	require.NoError(t, err, "scoped node priv validator should be set")

	assert.Equal(t, expectedValidators, len(actualGenesisDoc.Validators), "validator set should contain validators and priv validator")

	findPrivValidatorAddress := slices.IndexFunc(actualValidators, func(v types.GenesisValidator) bool {
		return v.PubKey.Address().String() == actualPrivValPub.Address().String()
	})

	assert.NotEqual(t, -1, findPrivValidatorAddress)
}

func TestMultiplexValidatorSetGenesisUsesCorrectValidatorsTwoChains(t *testing.T) {

	testName := "mx_test_validator_set_genesis_uses_correct_validators_two_chains"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
	}

	validatorsPubs, _ := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
		nil,
		0, // 0-startPortOverwrite (defaults to 30001)
	)

	expectedNumChains := 2
	expectedValidators := numValidators + 1 // priv validator
	require.Equal(t, expectedNumChains, len(r.Nodes))
	require.Contains(t, r.Nodes, mxtest.TestScopeHash, "nodes registry should contain TestScopeHash")

	// Asserts the number of validators for each chain and
	// asserts that the priv validators are present in validators
	for _, node := range r.Nodes {
		actualGenesisDoc := node.GenesisDoc()
		actualValidators := actualGenesisDoc.Validators

		actualPrivValPub, err := node.PrivValidator().GetPubKey()
		require.NoError(t, err, "scoped node priv validator should be set")

		assert.Equal(t, expectedValidators, len(actualGenesisDoc.Validators), "validator set should contain validators and priv validator")

		findPrivValidatorAddress := slices.IndexFunc(actualValidators, func(v types.GenesisValidator) bool {
			return v.PubKey.Address().String() == actualPrivValPub.Address().String()
		})

		assert.NotEqual(t, -1, findPrivValidatorAddress, "priv validator should be present in genesis validators")
	}
}

func TestMultiplexValidatorSetManyChainsHaveCorrectValidators(t *testing.T) {

	testName := "mx_test_validator_set_many_chains_have_correct_validators"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here (1st scope).
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "C0mpl3x Scope, by Midas!"},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Posts", "Likes", "Media", "Reviews", "Links"},
		"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ICE", "Osmosis"},
		"5168FD905426DE2E0DB9990B35075EEC3B184977": {"Posts", "Likes"},
	}

	validatorsPubs, _ := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
		nil,
		0, // 0-startPortOverwrite (defaults to 30001)
	)

	expectedNumChains := 12
	expectedValidators := numValidators + 1 // priv validator
	require.Equal(t, expectedNumChains, len(r.Nodes))
	require.Contains(t, r.Nodes, mxtest.TestScopeHash, "nodes registry should contain TestScopeHash")

	// Asserts the number of validators for each chain and
	// asserts that the priv validators are present in validators
	for _, node := range r.Nodes {
		actualGenesisDoc := node.GenesisDoc()
		actualValidators := actualGenesisDoc.Validators
		actualPrivValPub, err := node.PrivValidator().GetPubKey()
		require.NoError(t, err, "scoped node priv validator should be set")

		assert.Equal(t, expectedValidators, len(actualGenesisDoc.Validators), "validator set should contain validators and priv validator")

		findPrivValidatorAddress := slices.IndexFunc(actualValidators, func(v types.GenesisValidator) bool {
			return v.PubKey.Address().String() == actualPrivValPub.Address().String()
		})

		assert.NotEqual(t, -1, findPrivValidatorAddress, "priv validator should be present in genesis validators")
	}
}

func TestMultiplexValidatorSetStartStopSingleValidatorChain(t *testing.T) {

	testName := "mx_test_validator_set_start_stop_single_validator_chain"
	numValidators := 1
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)
	require.Contains(t, privValidators, mxtest.TestScopeHash)
	require.Equal(t, len(privValidators[mxtest.TestScopeHash]), numValidators)

	privValidator_n1 := privValidators[mxtest.TestScopeHash][0]

	// Configures one independent validator node
	c1, r1, s1 := assertConfigureMultiplexNodeRegistry(t,
		testName+"-0",
		userScopes,
		validatorsPubs,   // non-empty => ResetTestRootMultiplexWithValidators
		privValidator_n1, // sets custom privValidator
		0,                // 0-startPortOverwrite (defaults to 30001)
	)

	var wg sync.WaitGroup

	// Starts, produces block and stops the first validator node
	// Calls wg.Add() before start and wg.Done() after stop
	assertStartValidatorNode(t, c1, r1.Nodes[mxtest.TestScopeHash], &wg, s1)

	// Waits until the node produces a block and stops
	wg.Wait()
}

// ----------------------------------------------------------------------------

func createTestValidatorSet(
	t testing.TB,
	testName string,
	numValidators int,
	userScopes map[string][]string, // user_address:[scope1,scope2]
) (map[string][]string, map[string][]*privval.FilePV) {
	t.Helper()

	validatorSet := map[string][]string{}
	privValidatorSet := map[string][]*privval.FilePV{}

	for userAddress, scopes := range userScopes {
		for _, scope := range scopes {
			scopeHash := mxtest.CreateScopeHash(userAddress + ":" + scope)

			// Generates numValidators random priv validators
			privValidatorSet[scopeHash] = make([]*privval.FilePV, numValidators)
			privValidatorSet[scopeHash] = mxtest.GeneratePrivValidators(testName, numValidators)

			validatorSet[scopeHash] = make([]string, len(privValidatorSet[scopeHash]))

			for i, pv := range privValidatorSet[scopeHash] {
				pvPubKey := base64.StdEncoding.EncodeToString(pv.Key.PubKey.Bytes())
				validatorSet[scopeHash][i] = pvPubKey
			}
		}
	}

	return validatorSet, privValidatorSet
}

func assertStartValidatorNode(
	t testing.TB,
	config *cfg.Config,
	node *ScopedNode,
	wg *sync.WaitGroup,
	scopeRegistry *ScopeRegistry,
) {
	nodeDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)
	require.DirExists(t, nodeDataDir, "scoped node data dir should exist")

	wg.Add(1)
	t.Logf("Starting new node: %s - %s", node.ScopeHash, node.GenesisDoc().ChainID)

	walFolder := node.ScopeHash[:16] // uses hex

	// Overwrite wal file on a per-node basis
	walFile := filepath.Join(nodeDataDir, "cs.wal", walFolder, "wal")

	// We overwrite the wal file to allow parallel I/O for multiple nodes
	t.Logf("Using walFile: %s", walFile)
	node.Config().Consensus.SetWalFile(walFile)

	userAddress, err := scopeRegistry.GetAddress(node.ScopeHash)
	require.NoError(t, err)

	// We set the PrivValidator for every node and consensus reactors
	usePrivValidatorFromFiles(t, node, config, userAddress)

	// Calls wg.Done() after executing node.Stop()
	assertStartStopScopedNode(t, wg, node)
}
