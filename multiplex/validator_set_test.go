package multiplex

import (
	"encoding/base64"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mxtest "github.com/cometbft/cometbft/multiplex/test"
	types "github.com/cometbft/cometbft/types"
)

func TestMultiplexValidatorSetGenesisUsesCorrectValidators(t *testing.T) {

	testName := "mx_test_validator_set_single_chain"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	validatorsPubs := createTestValidatorSet(t, testName, 3, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
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

func TestMultiplexValidatorSetGenesisUsesCorrectValidatorsForMultipleChains(t *testing.T) {

	testName := "mx_test_validator_set_single_chain"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
	}

	validatorsPubs := createTestValidatorSet(t, testName, 3, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
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

// ----------------------------------------------------------------------------

func createTestValidatorSet(
	t testing.TB,
	testName string,
	numValidators int,
	userScopes map[string][]string, // user_address:[scope1,scope2]
) map[string][]string {
	t.Helper()

	validatorSet := map[string][]string{}

	for userAddress, scopes := range userScopes {
		for _, scope := range scopes {
			scopeHash := mxtest.CreateScopeHash(userAddress + ":" + scope)

			// Generates numValidators random priv validators
			privValidators := mxtest.GeneratePrivValidators(testName, numValidators)

			validatorSet[scopeHash] = make([]string, len(privValidators))
			for i, pv := range privValidators {
				pvPubKey := base64.StdEncoding.EncodeToString(pv.Key.PubKey.Bytes())
				validatorSet[scopeHash][i] = pvPubKey
			}
		}
	}

	return validatorSet
}
