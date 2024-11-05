package client_test

import (
	"fmt"
	"testing"

	"github.com/cometbft/cometbft/crypto/tmhash"
	"github.com/stretchr/testify/assert"

	"github.com/cometbft/cometbft/multiplex/client"
)

func TestMultiplexClientDelegateCheckTx(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainId := makeRandomTestChainID()

	// Prepare a base transaction
	baseTransaction := []byte(`this is not a real transaction.`)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errDelegateCheckTx error
	errDelegateCheckTx = client.DelegateCheckTx(
		testChainId,
		baseTransaction,
		mockCheckTxExtension_SizeAsError, // default_test.go
	)

	// The extension should have formatted the transaction size as an Error
	expectedMessage := fmt.Sprintf("Transaction bytes: %d", len(baseTransaction))
	assert.Equal(t, expectedMessage, errDelegateCheckTx.Error())
}

func TestMultiplexClientInjectPrepareProposal(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainId := makeRandomTestChainID()
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

func TestMultiplexClientInjectProcessProposal(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainId := makeRandomTestChainID()
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionBytes := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectProcessProposal [][]byte
	injectProcessProposal = client.InjectProcessProposal(
		testChainId,
		baseTransactionsSlice,
		mockProcessProposalExtension_AppendOneTx, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectProcessProposal, "ProcessProposalExtensionFn may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, injectProcessProposal)

	// The extension should have appended one transaction
	assert.Len(t, injectProcessProposal, len(inputTransactionBytes)+1)
	assert.NotEqual(t, baseTransactionsSlice, injectProcessProposal)

	// But it should not have touched the input config object
	assert.Equal(t, baseTransactionsSlice, inputTransactionBytes)
}

func TestMultiplexClientInjectFinalizeBlock(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainId := makeRandomTestChainID()
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

func TestMultiplexClientReportCommit(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainId := makeRandomTestChainID()

	// Prepare a base block height
	baseBlockHeight := uint64(123)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errReportCommit error
	errReportCommit = client.ReportCommit(
		testChainId,
		baseBlockHeight,
		mockCommitExtension_HeightAsError, // default_test.go
	)

	// The extension should have formatted the block height as an Error
	expectedMessage := fmt.Sprintf("Block height: %v", baseBlockHeight)
	assert.Equal(t, expectedMessage, errReportCommit.Error())
}
