package client_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/crypto/tmhash"
	"github.com/stretchr/testify/assert"

	"github.com/cometbft/cometbft/multiplex/client"
)

func TestMultiplexClientInjectSnapshotMutation(t *testing.T) {
	// address and fingerprint added to ChainID
	userPubKey := ed25519.GenPrivKey().PubKey()
	userAddress := userPubKey.Address().String()
	fingerprint := strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte("Posts"))[:8], // 8 bytes only
	))

	testChainId := "test-chain-" + userAddress + "-" + fingerprint
	baseStateBytes := []byte(`this is just an example, not a sm.State.`)
	inputStateBytes := baseStateBytes[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectSnapshotMutation []byte
	injectSnapshotMutation = client.InjectSnapshotMutation(
		testChainId,
		baseStateBytes,
		mockSnapshotMutationExtension_HashedState, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectSnapshotMutation, "SnapshotMutationExtensionFn may not return nil")

	// Must return a []byte
	assert.NotEmpty(t, injectSnapshotMutation)

	// The extension should have hashed stated (tmhash) and return it
	assert.Len(t, injectSnapshotMutation, tmhash.Size)
	assert.NotEqual(t, baseStateBytes, injectSnapshotMutation)

	// But it should not have touched the input config object
	assert.Equal(t, baseStateBytes, inputStateBytes)
}

func TestMultiplexClientInjectPrepareProposal(t *testing.T) {
	// address and fingerprint added to ChainID
	userPubKey := ed25519.GenPrivKey().PubKey()
	userAddress := userPubKey.Address().String()
	fingerprint := strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte("Posts"))[:8], // 8 bytes only
	))

	testChainId := "test-chain-" + userAddress + "-" + fingerprint
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionBytes := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectPrepareProposal [][]byte
	injectPrepareProposal = client.InjectPrepareProposal(
		testChainId,
		baseTransactionsSlice,
		mockPrepareProposalExtension_AppendOneTx, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectPrepareProposal, "PrepareProposalExtensionFn may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, injectPrepareProposal)

	// The extension should have appended one transaction
	assert.Len(t, injectPrepareProposal, len(inputTransactionBytes)+1)
	assert.NotEqual(t, baseTransactionsSlice, injectPrepareProposal)

	// But it should not have touched the input config object
	assert.Equal(t, baseTransactionsSlice, inputTransactionBytes)
}

func TestMultiplexClientInjectFinalizeBlock(t *testing.T) {
	// address and fingerprint added to ChainID
	userPubKey := ed25519.GenPrivKey().PubKey()
	userAddress := userPubKey.Address().String()
	fingerprint := strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte("Posts"))[:8], // 8 bytes only
	))

	testChainId := "test-chain-" + userAddress + "-" + fingerprint
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionBytes := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectFinalizeBlock [][]byte
	injectFinalizeBlock = client.InjectFinalizeBlock(
		testChainId,
		baseTransactionsSlice,
		mockFinalizeBlockExtension_AppendHash, // default_test.go
	)

	// The extension should have appended a transactions hash
	actualLastTx := injectFinalizeBlock[len(injectFinalizeBlock)-1]

	// Extension may not return nil
	assert.NotNil(t, injectFinalizeBlock, "FinalizeBlockExtensionFn may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, injectFinalizeBlock)

	// The extension should have appended the transactions hash
	assert.Len(t, injectFinalizeBlock, len(inputTransactionBytes)+1)
	assert.NotEqual(t, baseTransactionsSlice, injectFinalizeBlock)
	assert.Len(t, actualLastTx, tmhash.Size)

	// But it should not have touched the input config object
	assert.Equal(t, baseTransactionsSlice, inputTransactionBytes)
}
