package multiplex_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/cometbft/cometbft/crypto/ed25519"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	sm "github.com/cometbft/cometbft/state"
	types "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/multiplex/snapshots"
	snapshottypes "github.com/cometbft/cometbft/multiplex/snapshots/types"
)

var (
	stateKey = []byte("stateKey")
)

func TestMultiplexChainStateStoreCommit(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-app-hash", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainStore(t, multiplexDb)
	genState := makeGenesisState(t, "test-chain-0")
	otherState := makeGenesisState(t, "test-chain-0") // valPubKey is random
	chainStore, ok := multiplexStore["test-chain-0"]
	require.Equal(t, true, ok)
	chainStore.GetDatabase().Set(stateKey, genState.Bytes())

	// Should modify state and save updated state to database
	err := chainStore.Commit(otherState)
	assert.NoError(t, err)

	// Asserting results
	actualData, err := chainStore.GetDatabase().Get(stateKey)
	require.NoError(t, err)
	assert.Equal(t, true, bytes.Equal(otherState.Bytes(), actualData))
}

func TestMultiplexChainStateStoreLoad(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-app-hash", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainStore(t, multiplexDb)
	genState := makeGenesisState(t, "test-chain-0")
	chainStore, ok := multiplexStore["test-chain-0"]
	require.Equal(t, true, ok)
	chainStore.GetDatabase().Set(stateKey, genState.Bytes())

	// Should load the state instance from database
	loadedState, err := chainStore.Load()
	require.NoError(t, err)

	// Asserting results
	assert.Equal(t, true, bytes.Equal(genState.Bytes(), loadedState.Bytes()))
}

func TestMultiplexChainStateStoreAppHash(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-app-hash", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainStore(t, multiplexDb)
	genState := makeGenesisState(t, "test-chain-0")
	chainStore, ok := multiplexStore["test-chain-0"]
	require.Equal(t, true, ok)

	chainStore.GetDatabase().Set(stateKey, genState.Bytes())
	loadedState, err := chainStore.Load()
	require.NoError(t, err)

	// Should read correct AppHash from underlying State
	appHash := chainStore.AppHash()
	assert.Equal(t, true, bytes.Equal(loadedState.AppHash, appHash))

	loadedState.AppHash = []byte{3, 2, 1}
	chainStore.GetDatabase().Set(stateKey, loadedState.Bytes())

	// A change in AppHash should reflect as well
	modAppHash := chainStore.AppHash()
	assert.Equal(t, true, bytes.Equal(loadedState.AppHash, modAppHash))
}

func TestMultiplexChainStateStoreSnapshot(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-snapshot", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainStore(t, multiplexDb)
	genState := makeGenesisState(t, "test-chain-0")
	chainStore, ok := multiplexStore["test-chain-0"]
	require.Equal(t, true, ok)

	chainStore.GetDatabase().Set(stateKey, genState.Bytes())

	// ----------------
	// Errors

	chunks_1 := make(chan io.ReadCloser)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_1)
		defer streamWriter.Close()
		require.NotNil(t, streamWriter)

		// should error with invalid 0-height
		err := chainStore.Snapshot(0, streamWriter)
		assert.Error(t, err)
	}()

	chunks_2 := make(chan io.ReadCloser)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_2)
		defer streamWriter.Close()

		require.NotNil(t, streamWriter)

		genState.LastBlockHeight = 1000
		chainStore.GetDatabase().Set(stateKey, genState.Bytes())

		// should error with invalid future height
		err := chainStore.Snapshot(1001, streamWriter)
		assert.Error(t, err)
	}()

	// ----------------
	// OK

	chunks_3 := make(chan io.ReadCloser)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_3)
		defer streamWriter.Close()
		require.NotNil(t, streamWriter)

		newState := makeSnapshottableState(t, "test-chain-0", 1010)
		chainStore.GetDatabase().Set(stateKey, newState.Bytes())

		err := chainStore.Snapshot(uint64(1010), streamWriter)
		require.NoError(t, err)
	}()

	for reader := range chunks_3 {
		_, err := io.Copy(io.Discard, reader)
		require.NoError(t, err)
		err = reader.Close()
		require.NoError(t, err)
	}
}

func TestMultiplexChainStateStoreSnapshotRestore(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-restore", 2)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainStore(t, multiplexDb)
	newState := makeSnapshottableState(t, "test-chain-0", 1010)
	chainStore, ok := multiplexStore["test-chain-0"]
	require.Equal(t, true, ok)

	// using 2 different state instances (but same ChainID)
	otherState := makeSnapshottableState(t, "test-chain-0", 500)
	otherState.AppHash = []byte{3, 2, 1} // different before restoration
	otherStore, ok := multiplexStore["test-chain-1"]
	require.Equal(t, true, ok)

	chainStore.GetDatabase().Set(stateKey, newState.Bytes())
	otherStore.GetDatabase().Set(stateKey, otherState.Bytes())

	chunks := make(chan io.ReadCloser)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks)
		defer streamWriter.Close()
		require.NotNil(t, streamWriter)

		// first take a snapshot
		err := chainStore.Snapshot(uint64(1010), streamWriter)
		require.NoError(t, err)
	}()

	reader, err := snapshots.NewStreamReader(chunks)
	require.NoError(t, err)

	// .. and try to restore it (on different store instance for tests)
	_, err = otherStore.Restore(1010, snapshottypes.CurrentNetworkFormat, reader)
	require.NoError(t, err)

	// Must have loaded the AppHash from the restored snapshot
	assert.Equal(t, true, bytes.Equal(chainStore.AppHash(), otherStore.AppHash()))

	// Should have reloaded new snapshotted state
	reloadedState, err := otherStore.GetStateMachine()
	require.NoError(t, err)
	assert.Equal(t, true, bytes.Equal(newState.Bytes(), reloadedState.Bytes()))
}

func TestMultiplexChainStateStoreNewMultiplexChainStore(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-newmultiplexchainstore", 2)
	defer os.RemoveAll(rootDir)

	multiplexStore := mx.NewMultiplexChainStore(
		multiplexDb,
		sm.StoreOptions{
			DiscardABCIResponses: false,
			DBKeyLayout:          "v2",
		},
		cmtlog.NewNopLogger(),
	)

	assert.Len(t, multiplexStore, 2)

	for testChainId, testStore := range multiplexStore {
		assert.Equal(t, testChainId, testStore.ChainID)
		assert.NotNil(t, testStore.DBStore)
	}
}

func TestMultiplexChainStateStoreNewChainStateStore(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-newchainstatestore", 2)
	defer os.RemoveAll(rootDir)

	multiplexStore := mx.NewMultiplexChainStore(
		multiplexDb,
		sm.StoreOptions{
			DiscardABCIResponses: false,
			DBKeyLayout:          "v2",
		},
		cmtlog.NewNopLogger(),
	)
	require.Len(t, multiplexStore, 2)

	// Should error given unknown ChainID
	_, err := mx.NewChainStateStore(multiplexStore, "unknown-chain")
	assert.Error(t, err, "should error given unknown ChainID")

	// Should succeed using correct ChainID
	testStore, err := mx.NewChainStateStore(multiplexStore, "test-chain-0")
	assert.NoError(t, err, "should find ChainStateStore in multiplex")
	assert.Equal(t, "test-chain-0", testStore.ChainID)
	assert.NotNil(t, testStore.DBStore)
}

// ----------------------------------------------------------------------------

func makeMultiplexChainStore(t *testing.T, multiplexDb mx.MultiplexDB) mx.MultiplexChainStore {
	t.Helper()

	return mx.NewMultiplexChainStore(
		multiplexDb,
		sm.StoreOptions{
			DiscardABCIResponses: false,
			DBKeyLayout:          "v2",
		},
		cmtlog.NewNopLogger(),
	)
}

func makeGenesisState(t *testing.T, chainId string) sm.State {
	t.Helper()

	valPubKey := ed25519.GenPrivKey().PubKey()
	state, err := sm.MakeGenesisState(&types.GenesisDoc{
		GenesisTime:   cmttime.Now(),
		ChainID:       chainId,
		InitialHeight: 1000,
		Validators: []types.GenesisValidator{{
			Address: valPubKey.Address(),
			PubKey:  valPubKey,
			Power:   10,
			Name:    "myval",
		}},
		ConsensusParams: types.DefaultConsensusParams(),
		AppHash:         []byte{1, 2, 3},
		AppState:        []byte(`{"account_owner":"Bob"}`),
	})
	require.NoError(t, err)

	return state
}

func makeSnapshottableState(
	t *testing.T,
	chainId string,
	setHeight int64,
) sm.State {
	t.Helper()

	state := makeGenesisState(t, chainId)

	state.LastBlockHeight = setHeight
	state.LastBlockID = types.BlockID{}
	state.LastBlockTime = cmttime.Now()
	state.LastValidators = state.Validators

	return state
}
