package snapsapp_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/cometbft/cometbft/api/cometbft/abci/v1"
	"github.com/cometbft/cometbft/crypto/ed25519"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

var (
	stateKey = []byte("stateKey")
)

func TestABCI_Info(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	require.NotNil(t, suite.chainStores)
	require.Contains(t, suite.chainStores, exampleChainID)

	reqTestInfo := abci.InfoRequest{}

	// Store custom state machine instance
	expectHeight := int64(1500)
	appState, appHash := makeState(t, exampleChainID, expectHeight)
	suite.chainStores[exampleChainID].GetDatabase().Set(stateKey, appState.Bytes())

	// Should return empty given invalid ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", "unknown-chain")
	infoRes, err := suite.snapsApp.Info(ctx, &reqTestInfo)
	assert.Nil(t, err, "should not error given Info request")
	assert.Empty(t, infoRes.GetData())

	// Should succeed given valid injected ChainID
	ctx = context.WithValue(ctx, "ChainID", exampleChainID)
	infoRes, err = suite.snapsApp.Info(ctx, &reqTestInfo)
	assert.NoError(t, err, "should not error given Info request")
	assert.Equal(t, exampleChainID, infoRes.GetData())
	assert.Equal(t, appHash, infoRes.GetLastBlockAppHash())
	assert.Equal(t, expectHeight, infoRes.GetLastBlockHeight())
}

func TestABCI_InitChain(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	require.NotNil(t, suite.chainStores)
	require.Contains(t, suite.chainStores, exampleChainID)

	// Store custom state machine instance
	emptyState := &sm.State{}
	suite.chainStores[exampleChainID].GetDatabase().Set(stateKey, emptyState.Bytes())

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
		ChainID:       exampleChainID,
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
	suite.chainStores[exampleChainID].GetDatabase().Set(stateKey, genState.Bytes())

	// Should succeed given corret ChainID
	initChainRes, err = suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		AppStateBytes: []byte("{}"),
		ChainId:       exampleChainID, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, fakeAppHash, initChainRes.AppHash)
}

func TestABCI_InitChain_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	require.NotNil(t, suite.chainStores)
	require.Contains(t, suite.chainStores, exampleChainID)

	// Attach an Initial Height
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		InitialHeight: 3,
		AppStateBytes: []byte("{}"),
		ChainId:       exampleChainID, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), suite.snapsApp.LastBlockHeight(exampleChainID))
}

func TestABCI_FinalizeBlock_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	require.NotNil(t, suite.chainStores)
	require.Contains(t, suite.chainStores, exampleChainID)

	// Attach an Initial Height
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		InitialHeight: 3,
		AppStateBytes: []byte("{}"),
		ChainId:       exampleChainID, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), suite.snapsApp.LastBlockHeight(exampleChainID))

	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", exampleChainID)

	_, err = suite.snapsApp.FinalizeBlock(ctx, &abci.FinalizeBlockRequest{Height: 4})
	require.Error(t, err, "invalid height: 4; expected: 3")
}

func TestABCI_Proposal_HappyPath(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	require.NotNil(t, suite.chainStores)
	require.Contains(t, suite.chainStores, exampleChainID)

	// ---------------------
	// (1). InitChain
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		ChainId: exampleChainID,
	})
	assert.NoError(t, err, "should not error given correct ChainID (InitChain)")

	// ---------------------
	// (2). PrepareProposal
	bytes_tx1 := []byte{1, 2, 3}
	bytes_tx2 := []byte{4, 5, 6}
	reqPrepareProposal := abci.PrepareProposalRequest{
		MaxTxBytes: 1000,
		Height:     1,
		Txs:        [][]byte{bytes_tx1, bytes_tx2},
	}
	resPrepareProposal, err := suite.snapsApp.PrepareProposal(context.TODO(), &reqPrepareProposal)
	assert.NoError(t, err, "should not error given proposal request (PrepareProposal)")
	assert.Equal(t, 2, len(resPrepareProposal.Txs))

	// ---------------------
	// (3). ProcessProposal
	reqProposalMergedTxBytes := [2][]byte{bytes_tx1, bytes_tx2}
	reqProcessProposal := abci.ProcessProposalRequest{
		Txs:    reqProposalMergedTxBytes[:],
		Height: reqPrepareProposal.Height,
	}

	ctx := context.TODO()
	ctx = context.WithValue(ctx, "ChainID", exampleChainID)

	resProcessProposal, err := suite.snapsApp.ProcessProposal(ctx, &reqProcessProposal)
	assert.NoError(t, err, "should not error given proposal request (ProcessProposal)")
	assert.Equal(t, abci.PROCESS_PROPOSAL_STATUS_ACCEPT, resProcessProposal.Status)

	// ---------------------
	// (4). FinalizeBlock
	lastBlockHeight := suite.snapsApp.LastBlockHeight(exampleChainID)
	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &abci.FinalizeBlockRequest{
		Height: lastBlockHeight + 1,
		Txs:    reqProposalMergedTxBytes[:], // same as ProcessProposal
	})
	assert.NoError(t, err, "should not error given correct request (FinalizeBlock)")
	assert.NotEmpty(t, resFinalizeBlock.TxResults)
	assert.Len(t, resFinalizeBlock.TxResults, 2)
}
