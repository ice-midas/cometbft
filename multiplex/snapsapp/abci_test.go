package snapsapp_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/cometbft/cometbft/api/cometbft/abci/v1"
	"github.com/cometbft/cometbft/crypto/ed25519"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"

	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/multiplex/client"
	"github.com/cometbft/cometbft/multiplex/snapsapp"
)

var (
	stateKey = []byte("stateKey")

	testPanicMessage = "A panic which occurs in a ABCI extension"
)

// ----------------------------------------------------------------------------
// Mocks

// Type-assertions ensure the compatibility of these mocks with the
// multiplex client contract defined in the client package.
var _ client.CheckTxExtensionFn = mockCheckTxExtension_WithError
var _ client.PrepareProposalExtensionFn = mockPrepareProposalExtension_WithError
var _ client.ProcessProposalExtensionFn = mockProcessProposalExtension_WithError
var _ client.FinalizeBlockExtensionFn = mockFinalizeBlockExtension_WithError
var _ client.CommitExtensionFn = mockCommitExtension_WithError

// mockPrepareProposalExtension_WithError is an implementation that deep-copies
// the transactions bytes slices and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockPrepareProposalExtension_WithError(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := baseTransactions[:]
	panic(errors.New(testPanicMessage))
	return nextTransactions
}

// mockProcessProposalExtension_WithError is an implementation that deep-copies
// the transactions bytes slices and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockProcessProposalExtension_WithError(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := baseTransactions[:]
	panic(errors.New(testPanicMessage))
	return nextTransactions
}

// mockFinalizeBlockExtension_WithError is an implementation that deep-copies
// the transactions bytes slices and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockFinalizeBlockExtension_WithError(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := baseTransactions[:]
	panic(errors.New(testPanicMessage))
	return nextTransactions
}

// mockCheckTxExtension_WithError is an implementation that deep-copies
// the transaction bytes and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockCheckTxExtension_WithError(
	ctx context.Context,
	baseTransaction []byte,
) error {
	nextTransactions := baseTransaction[:]
	func(_ []byte) {}(nextTransactions)
	panic(errors.New(testPanicMessage))
	return nil
}

// mockCommitExtension_WithError is an implementation which just panics
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockCommitExtension_WithError(
	ctx context.Context,
	_ uint64,
) error {
	panic(errors.New(testPanicMessage))
	return nil
}

// ----------------------------------------------------------------------------
// Unit tests

func TestABCI_Info(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	// We must cast to ChainHistoryStore for database access
	chainStore := suite.reactor.GetStateStore(testChainId).(*mx.ChainHistoryStore)
	require.NotNil(t, chainStore)

	reqTestInfo := abci.InfoRequest{}

	// Store custom state machine instance
	expectHeight := int64(1500)
	appState, appHash := makeState(t, testChainId, expectHeight)
	chainStore.GetDatabase().Set(stateKey, appState.Bytes())

	// Should return empty given invalid ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", "unknown-chain")
	infoRes, err := suite.snapsApp.Info(ctx, &reqTestInfo)
	assert.Nil(t, err, "should not error given Info request")
	assert.Empty(t, infoRes.GetData())

	// Should succeed given valid injected ChainID
	ctx = context.WithValue(ctx, "ChainID", testChainId)
	infoRes, err = suite.snapsApp.Info(ctx, &reqTestInfo)
	assert.NoError(t, err, "should not error given Info request")
	assert.Equal(t, testChainId, infoRes.GetData())
	assert.Equal(t, appHash, infoRes.GetLastBlockAppHash())
	assert.Equal(t, expectHeight, infoRes.GetLastBlockHeight())
}

func TestABCI_InitChain(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	// We must cast to ChainHistoryStore for database access
	chainStore := suite.reactor.GetStateStore(testChainId).(*mx.ChainHistoryStore)
	require.NotNil(t, chainStore)

	// Store custom state machine instance
	emptyState := &mx.HistoricalState{State: &sm.State{}, Data: []byte{}}
	chainStore.GetDatabase().Set(stateKey, emptyState.Bytes())

	// Should error given unknown ChainID
	initChainRes, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		ChainId: "wrong-chain-id",
	})
	assert.Error(t, err)

	// Store custom GENESIS state for InitChain
	valPubKey := ed25519.GenPrivKey().PubKey()
	fakeAppHash := []byte{1, 2, 3}
	genState, err := sm.MakeGenesisState(&types.GenesisDoc{
		GenesisTime:   cmttime.Now(),
		ChainID:       testChainId,
		InitialHeight: 0,
		Validators: []types.GenesisValidator{{
			Address: valPubKey.Address(),
			PubKey:  valPubKey,
			Power:   10,
			Name:    "myval",
		}},
		ConsensusParams: types.DefaultConsensusParams(),
		AppHash:         fakeAppHash,
		AppState:        []byte(`{}`),
	})
	require.NoError(t, err, "should not error creating state machine")

	archiveState := &mx.HistoricalState{
		State: &genState,
		Data:  []byte{},
	}
	chainStore.GetDatabase().Set(stateKey, archiveState.Bytes())

	// Should succeed given corret ChainID
	initChainRes, err = suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		AppStateBytes: []byte("{}"),
		ChainId:       testChainId, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, fakeAppHash, initChainRes.AppHash)
}

func TestABCI_InitChain_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// Attach an Initial Height
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		InitialHeight: 3,
		AppStateBytes: []byte("{}"),
		ChainId:       testChainId, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), suite.snapsApp.LastBlockHeight(testChainId))
}

func TestABCI_PrepareProposal(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// (1). PrepareProposal
	bytes_tx1 := []byte{1, 2, 3}
	bytes_tx2 := []byte{4, 5, 6}
	reqPrepareProposal := abci.PrepareProposalRequest{
		MaxTxBytes: 1000,
		Height:     1,
		Txs:        [][]byte{bytes_tx1, bytes_tx2},
	}

	resPrepareProposal, err := suite.snapsApp.PrepareProposal(ctx, &reqPrepareProposal)
	assert.NoError(t, err, "should not error given proposal request (PrepareProposal)")
	assert.Equal(t, 2, len(resPrepareProposal.Txs))
}

func TestABCI_PrepareProposal_ExtensionFailure(t *testing.T) {
	// Forces a FAILING PrepareProposal extension
	suite := NewSnapsAppSuite(t, snapsapp.WithPrepareProposalExtension(
		mockPrepareProposalExtension_WithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// ---------------------
	// Errors
	//
	// - Must error and stop the block proposal process given faulty extension.

	expectedError := fmt.Sprintf(
		"CLIENT PANIC: failing prepare proposal extension: %s", testPanicMessage)

	// (1). PrepareProposal
	bytes_tx1 := []byte{1, 2, 3}
	bytes_tx2 := []byte{4, 5, 6}
	reqPrepareProposal := abci.PrepareProposalRequest{
		MaxTxBytes: 1000,
		Height:     1,
		Txs:        [][]byte{bytes_tx1, bytes_tx2},
	}

	// Should error given a failing PrepareProposal extension
	_, errPrepareProposal := suite.snapsApp.PrepareProposal(ctx, &reqPrepareProposal)

	// Returning an error means that the proposal process is STOPPED!
	assert.Error(t, errPrepareProposal,
		"should stop blocks proposal process given failing extension")
	assert.Equal(t, expectedError, errPrepareProposal.Error())
}

func TestABCI_ProcessProposal(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// (1). ProcessProposal
	bytes_tx1 := []byte{1, 2, 3}
	bytes_tx2 := []byte{4, 5, 6}
	mergedTxBytes := [2][]byte{bytes_tx1, bytes_tx2}
	reqProcessProposal := abci.ProcessProposalRequest{
		Txs:    mergedTxBytes[:],
		Height: 1,
	}

	resProcessProposal, err := suite.snapsApp.ProcessProposal(ctx, &reqProcessProposal)
	assert.NoError(t, err, "should not error given proposal request (ProcessProposal)")
	assert.Equal(t, abci.PROCESS_PROPOSAL_STATUS_ACCEPT, resProcessProposal.Status)
}

func TestABCI_ProcessProposal_ExtensionFailure(t *testing.T) {
	// Forces a FAILING ProcessProposal extension
	suite := NewSnapsAppSuite(t, snapsapp.WithProcessProposalExtension(
		mockProcessProposalExtension_WithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// ---------------------
	// Errors
	//
	// - Must *not* influence the processing stage, i.e. should accept proposal

	// (1). ProcessProposal
	bytes_tx1 := []byte{1, 2, 3}
	bytes_tx2 := []byte{4, 5, 6}
	mergedTxBytes := [2][]byte{bytes_tx1, bytes_tx2}
	reqProcessProposal := abci.ProcessProposalRequest{
		Txs:    mergedTxBytes[:],
		Height: 1,
	}

	// Should not error, even with failing extension
	resProcessProposal, err := suite.snapsApp.ProcessProposal(ctx, &reqProcessProposal)
	assert.NoError(t, err, "should not error given proposal request (ProcessProposal)")
	assert.Equal(t, abci.PROCESS_PROPOSAL_STATUS_ACCEPT, resProcessProposal.Status)
}

func TestABCI_FinalizeBlock(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)
	assert.NoError(t, err)
	assert.NotNil(t, resFinalizeBlock)
}

func TestABCI_FinalizeBlock_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// Attach an Initial Height
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		InitialHeight: 3,
		AppStateBytes: []byte("{}"),
		ChainId:       testChainId, // must have valid JSON genesis file, even if empty
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), suite.snapsApp.LastBlockHeight(testChainId))

	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	res, err := suite.snapsApp.FinalizeBlock(ctx, &abci.FinalizeBlockRequest{Height: 4})
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

func TestABCI_FinalizeBlock_ExtensionFailure(t *testing.T) {
	// Forces a FAILING FinalizeBlock extension
	suite := NewSnapsAppSuite(t, snapsapp.WithFinalizeBlockExtension(
		mockFinalizeBlockExtension_WithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// ---------------------
	// Errors
	//
	// - Must error and stop the block finalization process given faulty extension.

	expectedError := fmt.Sprintf(
		"CLIENT PANIC: failing finalize block extension: %s", testPanicMessage)

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	// Should error given a failing FinalizeBlock extension
	_, errFinalizeBlock := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)

	// Returning an error means that the finalization process is STOPPED!
	assert.Error(t, errFinalizeBlock,
		"should stop finalization process given failing extension")
	assert.Equal(t, expectedError, errFinalizeBlock.Error())
}

func TestABCI_Proposal_HappyPath(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// (1). InitChain
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		ChainId: testChainId,
	})
	assert.NoError(t, err, "should not error given correct ChainID (InitChain)")

	// (2). PrepareProposal
	bytes_tx1 := []byte{1, 2, 3}
	bytes_tx2 := []byte{4, 5, 6}
	reqPrepareProposal := abci.PrepareProposalRequest{
		MaxTxBytes: 1000,
		Height:     1,
		Txs:        [][]byte{bytes_tx1, bytes_tx2},
	}

	resPrepareProposal, err := suite.snapsApp.PrepareProposal(ctx, &reqPrepareProposal)
	assert.NoError(t, err, "should not error given proposal request (PrepareProposal)")
	assert.Equal(t, 2, len(resPrepareProposal.Txs))

	// (3). ProcessProposal
	reqProposalMergedTxBytes := [2][]byte{bytes_tx1, bytes_tx2}
	reqProcessProposal := abci.ProcessProposalRequest{
		Txs:    reqProposalMergedTxBytes[:],
		Height: reqPrepareProposal.Height,
	}

	resProcessProposal, err := suite.snapsApp.ProcessProposal(ctx, &reqProcessProposal)
	assert.NoError(t, err, "should not error given proposal request (ProcessProposal)")
	assert.Equal(t, abci.PROCESS_PROPOSAL_STATUS_ACCEPT, resProcessProposal.Status)

	// (4). FinalizeBlock
	lastBlockHeight := suite.snapsApp.LastBlockHeight(testChainId)
	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &abci.FinalizeBlockRequest{
		Height: lastBlockHeight + 1,
		Txs:    reqProposalMergedTxBytes[:], // same as ProcessProposal
	})
	assert.NoError(t, err, "should not error given correct request (FinalizeBlock)")
	assert.NotEmpty(t, resFinalizeBlock.TxResults)
	assert.Len(t, resFinalizeBlock.TxResults, 2)
}

func TestABCI_CheckTx(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// (1). CheckTx
	testTransaction := []byte{1, 2, 3}
	reqCheckTx := abci.CheckTxRequest{
		Tx: testTransaction,
	}

	resCheckTx, errCheckTx := suite.snapsApp.CheckTx(ctx, &reqCheckTx)
	assert.NoError(t, errCheckTx, "should not error given check request (CheckTx)")
	assert.Equal(t, abci.CodeTypeOK, resCheckTx.Code)
}

func TestABCI_CheckTx_ExtensionFailure(t *testing.T) {
	// Forces a FAILING CheckTx extension
	suite := NewSnapsAppSuite(t, snapsapp.WithCheckTxExtension(
		mockCheckTxExtension_WithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// ---------------------
	// Errors
	//
	// - Must consider a transaction invalid given faulty extension

	// (1). CheckTx
	testTransaction := []byte{1, 2, 3}
	reqCheckTx := abci.CheckTxRequest{
		Tx: testTransaction,
	}

	resCheckTx, errCheckTx := suite.snapsApp.CheckTx(ctx, &reqCheckTx)
	assert.Error(t, errCheckTx, "should error given failing extension (CheckTx)")
	assert.NotEqual(t, abci.CodeTypeOK, resCheckTx.Code)
}

func TestABCI_Commit(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)
	require.NoError(t, err)
	require.NotNil(t, resFinalizeBlock)

	// (2). Commit
	resCommit, errCommit := suite.snapsApp.Commit(ctx, &abci.CommitRequest{})
	assert.NoError(t, errCommit, "should not error given commit request")
	assert.Equal(t, int64(0), resCommit.RetainHeight,
		"should use 0 as RetainHeight due to disabled pruning")
}

func TestABCI_Commit_ExtensionFailure(t *testing.T) {
	suite := NewSnapsAppSuite(t, snapsapp.WithCommitExtension(
		mockCommitExtension_WithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainId := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainId)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", testChainId)

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)
	require.NoError(t, err)
	require.NotNil(t, resFinalizeBlock)

	// ---------------------
	// Errors
	//
	// - Must *not* influence the commitment stage, i.e. should commit.

	// (2). Commit
	resCommit, errCommit := suite.snapsApp.Commit(ctx, &abci.CommitRequest{})
	assert.NoError(t, errCommit, "should not error given commit request")
	assert.Equal(t, int64(0), resCommit.RetainHeight,
		"should use 0 as RetainHeight due to disabled pruning")
}
