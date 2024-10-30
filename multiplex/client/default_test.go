package client_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

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
