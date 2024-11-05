package client_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/cometbft/cometbft/abci/types"
	v1 "github.com/cometbft/cometbft/api/cometbft/types/v1"
	cmtjson "github.com/cometbft/cometbft/libs/json"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"

	"github.com/cometbft/cometbft/multiplex/client"
)

const (
	testSeedNodesExample = "testNodeId@127.0.0.1:123,"
)

// ----------------------------------------------------------------------------
// Mocks

// mockSyncConfigExtension_MutatesHeight is an implementation that mutates the
// baseSyncConf.TrustHeight and increases it by 1.
func mockSyncConfigExtension_MutatesHeight(
	ctx context.Context,
	baseSyncConf *config.StateSyncConfig,
) *config.StateSyncConfig {
	nextStateSyncConfig := &config.StateSyncConfig{
		Enable:              baseSyncConf.Enable,
		TempDir:             baseSyncConf.TempDir,
		RPCServers:          baseSyncConf.RPCServers,
		TrustPeriod:         baseSyncConf.TrustPeriod,
		TrustHeight:         baseSyncConf.TrustHeight + 1, // mutation
		TrustHash:           baseSyncConf.TrustHash,
		DiscoveryTime:       baseSyncConf.DiscoveryTime,
		ChunkRequestTimeout: baseSyncConf.ChunkRequestTimeout,
		ChunkFetchers:       baseSyncConf.ChunkFetchers,
	}
	return nextStateSyncConfig
}

// mockSeedConfigExtension_PrefixOneSeed is an implementation that mutates the
// baseSeeds and prefixes it by adding "testNodeId@127.0.0.1:123,".
func mockSeedConfigExtension_PrefixOneSeed(
	ctx context.Context,
	baseSeeds string,
) string {
	nextSeeds := testSeedNodesExample + baseSeeds[:]
	return nextSeeds
}

// mockValidatorUpdateExtension_CountAsError is an implementation that reads the
// baseValidators validator set and formats an error with the number of validators.
func mockValidatorUpdateExtension_CountAsError(
	ctx context.Context,
	baseValidators []abci.ValidatorUpdate,
) error {
	return fmt.Errorf("Count validator updates: %d", len(baseValidators))
}

// mockConsensusUpdateExtension_MaxBytesAsError is an implementation that reads the
// consensusParams updates and formats an error with the max bytes content.
func mockConsensusUpdateExtension_MaxBytesAsError(
	ctx context.Context,
	baseConsensusParams *v1.ConsensusParams,
) error {
	return fmt.Errorf("Max bytes: %v", baseConsensusParams.Block.MaxBytes)
}

// mockSnapshotMutationExtension_HashedState is an implementation that mutates the
// baseState by hashing it and returning *only its hash*.
// CAUTION: this mock discards the state data for a deterministic hash of it.
func mockSnapshotMutationExtension_HashedState(
	ctx context.Context,
	baseState []byte,
) []byte {
	nextState := tmhash.Sum(baseState[:])
	return nextState
}

// mockSnapshotRestoreExtension_HashedState is an implementation that mutates the
// baseState by hashing it and returning *only its hash*.
// CAUTION: this mock discards the state data for a deterministic hash of it.
func mockSnapshotRestoreExtension_HashedState(
	ctx context.Context,
	baseState []byte,
) []byte {
	nextState := tmhash.Sum(baseState[:])
	return nextState
}

// mockCheckTxExtension_SizeAsError is an implementation that reads the
// transaction bytes and formats an error that prints the length of the byte slice.
func mockCheckTxExtension_SizeAsError(
	ctx context.Context,
	tx []byte,
) error {
	return fmt.Errorf("Transaction bytes: %d", len(tx))
}

// mockPrepareProposalExtension_AppendOneTx is an implementation that mutates the
// transactions slice so that it contains one more testable transaction.
// CAUTION: this mock mutates the transactions data.
func mockPrepareProposalExtension_AppendOneTx(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := append(baseTransactions[:], []byte(`test transaction`))
	return nextTransactions
}

// mockProcessProposalExtension_AppendOneTx is an implementation that mutates the
// transactions slice so that it contains one more testable transaction.
// CAUTION: this mock mutates the transactions data.
func mockProcessProposalExtension_AppendOneTx(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := append(baseTransactions[:], []byte(`test transaction`))
	return nextTransactions
}

// mockFinalizeBlockExtension_AppendHash is an implementation that mutates the
// transactions slice so that it contains a transations hash at the end.
// CAUTION: this mock mutates the transactions data.
func mockFinalizeBlockExtension_AppendHash(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	// We create a sha-256 hash of the flattened transactions bytes
	var hashedTxs []byte
	for _, baseTx := range baseTransactions {
		hashedTxs = append(hashedTxs, baseTx...)
	}

	// And append the hash to the transactions slice
	nextTransactions := append(baseTransactions[:], tmhash.Sum(hashedTxs))
	return nextTransactions
}

// mockCommitExtension_HeightAsError is an implementation that reads the
// block height and formats an error to print it.
func mockCommitExtension_HeightAsError(
	ctx context.Context,
	blockHeight uint64,
) error {
	return fmt.Errorf("Block height: %v", blockHeight)
}

// ----------------------------------------------------------------------------
// Unit tests

func TestMultiplexClientDefaultSyncConfigExtension(t *testing.T) {
	baseTrustHeight := int64(123)
	baseTrustHash := makeDeterministicTrustHash("trust me!")

	baseConf := config.TestConfig()
	baseConf.StateSync.TrustHeight = baseTrustHeight
	baseConf.StateSync.TrustHash = baseTrustHash

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextSyncConf *config.StateSyncConfig
	nextSyncConf = client.DefaultSyncConfigExtension(context.TODO(), baseConf.StateSync)
	// Should deep-copy the object
	// do some mutations to test deep-copy
	nextSyncConf.TrustHeight = baseTrustHeight + 1
	nextSyncConf.TrustHash = makeDeterministicTrustHash("do not trust me!")

	// Extension may not return nil
	assert.NotNil(t, nextSyncConf, "DefaultSyncConfigExtension may not return nil")

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input config object.
	assert.Equal(t, baseTrustHeight, baseConf.StateSync.TrustHeight)
	assert.Equal(t, baseTrustHash, baseConf.StateSync.TrustHash)
}

func TestMultiplexClientDefaultSeedConfigExtension(t *testing.T) {
	baseSeeds := "testNodeId2@192.168.1.1:30001,testNodeId3@192.168.1.2:30001"

	baseConf := config.TestConfig()
	baseConf.P2P.Seeds = baseSeeds

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextChainSeeds string
	nextChainSeeds = client.DefaultSeedConfigExtension(context.TODO(), baseConf.P2P.Seeds)
	// Should deep-copy the string
	// do some mutations to test deep-copy
	nextChainSeeds += ",mutationForTest"

	// Extension may not return nil
	assert.NotNil(t, nextChainSeeds, "DefaultSeedConfigExtension may not return nil")

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input config object.
	assert.Equal(t, baseSeeds, baseConf.P2P.Seeds)
}

func TestMultiplexClientDefaultValidatorUpdateExtension(t *testing.T) {
	// unmarshal a test validator
	testValidator := abci.ValidatorUpdate{}
	err := cmtjson.Unmarshal([]byte(testValidatorJSON), &testValidator)
	require.NoError(t, err, "should unmarshal a test validator from JSON")

	// Prepare a validators update set
	baseValidators := []abci.ValidatorUpdate{testValidator}

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errValidatorUpdate error
	errValidatorUpdate = client.DefaultValidatorUpdateExtension(context.TODO(), baseValidators)

	// Default extension returns nil (no error)
	assert.Nil(t, errValidatorUpdate, "DefaultValidatorUpdateExtension must return nil")
}

func TestMultiplexClientDefaultConsensusUpdateExtension(t *testing.T) {
	// Prepare a consensus params instance
	expectedMaxBytes := 123
	baseConsensusParams := &v1.ConsensusParams{
		Block: &v1.BlockParams{
			MaxBytes: int64(expectedMaxBytes),
		},
	}

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errConsensusUpdate error
	errConsensusUpdate = client.DefaultConsensusUpdateExtension(context.TODO(), baseConsensusParams)

	// Default extension returns nil (no error)
	assert.Nil(t, errConsensusUpdate, "DefaultConsensusUpdateExtension must return nil")
}

func TestMultiplexClientDefaultSnapshotMutationExtension(t *testing.T) {
	baseStateBytes := []byte(`this is just an example, not a sm.State.`)
	inputStateBytes := baseStateBytes[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextStateBytes []byte
	nextStateBytes = client.DefaultSnapshotMutationExtension(context.TODO(), inputStateBytes)
	// Should deep-copy the string
	// do some mutations to test deep-copy
	nextStateBytes = append(nextStateBytes, []byte(`adding more`)...)

	// Extension may not return nil
	assert.NotNil(t, nextStateBytes, "DefaultSnapshotMutationExtension may not return nil")

	// Must return a []byte
	assert.NotEmpty(t, nextStateBytes)

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slice.
	assert.Equal(t, baseStateBytes, inputStateBytes)
}

func TestMultiplexClientDefaultSnapshotRestoreExtension(t *testing.T) {
	baseStateBytes := []byte(`this is just an example, not a sm.State.`)
	inputStateBytes := baseStateBytes[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextStateBytes []byte
	nextStateBytes = client.DefaultSnapshotRestoreExtension(context.TODO(), inputStateBytes)
	// Should deep-copy the string
	// do some mutations to test deep-copy
	nextStateBytes = append(nextStateBytes, []byte(`adding more`)...)

	// Extension may not return nil
	assert.NotNil(t, nextStateBytes, "DefaultSnapshotRestoreExtension may not return nil")

	// Must return a []byte
	assert.NotEmpty(t, nextStateBytes)

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slice.
	assert.Equal(t, baseStateBytes, inputStateBytes)
}

func TestMultiplexClientDefaultCheckTxExtension(t *testing.T) {
	// Prepare a base transaction
	baseTransaction := []byte(`this is not a real transaction.`)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errCheckTx error
	errCheckTx = client.DefaultCheckTxExtension(context.TODO(), baseTransaction)

	// Default extension returns nil (no error)
	assert.Nil(t, errCheckTx, "DefaultCheckTxExtension must return nil")
}

func TestMultiplexClientDefaultPrepareProposalExtension(t *testing.T) {
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionsSlice := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextTransactions [][]byte
	nextTransactions = client.DefaultPrepareProposalExtension(context.TODO(), inputTransactionsSlice)

	// Extension may not return nil
	assert.NotNil(t, nextTransactions, "DefaultPrepareProposalExtension may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, nextTransactions)
	assert.Len(t, nextTransactions, len(baseTransactionsSlice))

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slices.
	assert.Equal(t, baseTransactionsSlice, inputTransactionsSlice)
}

func TestMultiplexClientDefaultProcessProposalExtension(t *testing.T) {
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionsSlice := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextTransactions [][]byte
	nextTransactions = client.DefaultProcessProposalExtension(context.TODO(), inputTransactionsSlice)

	// Extension may not return nil
	assert.NotNil(t, nextTransactions, "DefaultProcessProposalExtension may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, nextTransactions)
	assert.Len(t, nextTransactions, len(baseTransactionsSlice))

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slices.
	assert.Equal(t, baseTransactionsSlice, inputTransactionsSlice)
}

func TestMultiplexClientDefaultFinalizeBlockExtension(t *testing.T) {
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionsSlice := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextTransactions [][]byte
	nextTransactions = client.DefaultFinalizeBlockExtension(context.TODO(), inputTransactionsSlice)

	// Extension may not return nil
	assert.NotNil(t, nextTransactions, "DefaultFinalizeBlockExtension may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, nextTransactions)
	assert.Len(t, nextTransactions, len(baseTransactionsSlice))

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slices.
	assert.Equal(t, baseTransactionsSlice, inputTransactionsSlice)
}

func TestMultiplexClientDefaultCommitExtension(t *testing.T) {
	// Prepare a base block height
	baseBlockHeight := uint64(123)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errCommit error
	errCommit = client.DefaultCommitExtension(context.TODO(), baseBlockHeight)

	// Default extension returns nil (no error)
	assert.Nil(t, errCommit, "DefaultCommitExtension must return nil")
}
