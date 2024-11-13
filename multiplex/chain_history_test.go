package multiplex_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/crypto/tmhash"
	types "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"

	sm "github.com/cometbft/cometbft/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/multiplex/client"
	"github.com/cometbft/cometbft/multiplex/snapshots"
	snapshottypes "github.com/cometbft/cometbft/multiplex/snapshots/types"
)

var (
	stateKey          = []byte("stateKey")
	genesisDocKey     = []byte("mxGenesisDoc")
	genesisDocHashKey = []byte("mxGenesisDocHash")

	testPanicMessage = "A panic which occurs in a snapshot extension"
)

// ----------------------------------------------------------------------------
// Mocks

// Type-assertions ensure the compatibility of these mocks with the
// multiplex client contract defined in the client package.
var _ client.SnapshotMutationExtensionFn = mockSnapshotMutationExtension_WithError
var _ client.SnapshotMutationExtensionFn = mockSnapshotMutationExtension_HashedState
var _ client.SnapshotRestoreExtensionFn = mockSnapshotRestoreExtension_WithError
var _ client.SnapshotRestoreExtensionFn = mockSnapshotRestoreExtension_DeepCopy

// mockSnapshotMutationExtension_WithError is an implementation that mutates the
// baseState by hashing it and which panics afterwards.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockSnapshotMutationExtension_WithError(
	ctx context.Context,
	baseState []byte,
) []byte {
	nextState := tmhash.Sum(baseState[:])
	panic(errors.New(testPanicMessage))
	return nextState
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

// mockSnapshotRestoreExtension_WithError is an implementation that mutates the
// baseState by hashing it and which panics afterwards.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockSnapshotRestoreExtension_WithError(
	ctx context.Context,
	baseState []byte,
) []byte {
	nextState := tmhash.Sum(baseState[:])
	panic(errors.New(testPanicMessage))
	return nextState
}

// mockSnapshotRestoreExtension_DeepCopy is an implementation that deep-copies
// the baseState and returns it afterwards.
func mockSnapshotRestoreExtension_DeepCopy(
	ctx context.Context,
	baseState []byte,
) []byte {
	nextState := baseState[:]
	return nextState
}

// ----------------------------------------------------------------------------
// Unit tests

func TestMultiplexChainHistoryStoreCommit(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-app-hash", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainHistoryStore(t, multiplexDb)
	genState := makeHistoricalState(t, "test-chain-0")
	otherState := makeHistoricalState(t, "test-chain-0") // valPubKey is random
	chainStore, ok := multiplexStore["test-chain-0"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)
	chainStore.GetDatabase().Set(stateKey, genState.Bytes())

	// Should modify state and save updated state to database
	err := chainStore.Commit(*otherState)
	assert.NoError(t, err)

	// Asserting results
	actualData, err := chainStore.GetDatabase().Get(stateKey)
	require.NoError(t, err)
	assert.Equal(t, true, bytes.Equal(otherState.Bytes(), actualData))
}

func TestMultiplexChainHistoryStoreLoad(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-app-hash", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainHistoryStore(t, multiplexDb)
	genState := makeHistoricalState(t, "test-chain-0")
	chainStore, ok := multiplexStore["test-chain-0"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)
	chainStore.GetDatabase().Set(stateKey, genState.Bytes())

	// Should load the state machine from database
	loadedState, err := chainStore.Load()
	require.NoError(t, err)

	// Asserting results
	assert.Equal(t, true, bytes.Equal(genState.State.Bytes(), loadedState.Bytes()))
}

func TestMultiplexChainHistoryStoreAppHash(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-app-hash", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainHistoryStore(t, multiplexDb)
	genState := makeHistoricalState(t, "test-chain-0")
	chainStore, ok := multiplexStore["test-chain-0"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)

	chainStore.GetDatabase().Set(stateKey, genState.Bytes())
	loadedState, err := chainStore.Load()
	require.NoError(t, err)

	// Should read correct AppHash from underlying State
	appHash := chainStore.AppHash()
	assert.Equal(t, true, bytes.Equal(loadedState.AppHash, appHash))

	genState.AppHash = []byte{3, 2, 1}
	chainStore.GetDatabase().Set(stateKey, genState.Bytes())

	// A change in AppHash should reflect as well
	modAppHash := chainStore.AppHash()
	assert.Equal(t, true, bytes.Equal(genState.AppHash, modAppHash))
}

func TestMultiplexChainHistoryStoreSnapshot(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-snapshot", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainHistoryStore(t, multiplexDb)
	historicalState := makeHistoricalState(t, "test-chain-0")
	chainStore, ok := multiplexStore["test-chain-0"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)

	chainStore.GetDatabase().Set(stateKey, historicalState.Bytes())

	// Uses a WaitGroup to perform assertions sequentially
	// Note, this wait group is necessary to make sure we wait for all
	// assertions in the goroutine before we proceed with tests.
	var wg sync.WaitGroup

	// ----------------
	// Errors
	//
	// - Must error and stop the snapshotting process given a 0-height
	// - Must error and stop the snapshotting process given future height

	chunks_1 := make(chan io.ReadCloser)
	wg.Add(1)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_1)
		defer streamWriter.Close()
		require.NotNil(t, streamWriter)

		// Should error with invalid 0-height
		err := chainStore.Snapshot(0, streamWriter)
		assert.Error(t, err)

		// Defer order is LIFO
		defer wg.Done()
	}()
	wg.Wait()

	chunks_2 := make(chan io.ReadCloser)
	wg.Add(1)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_2)
		defer streamWriter.Close()

		require.NotNil(t, streamWriter)

		historicalState.LastBlockHeight = 1000
		chainStore.GetDatabase().Set(stateKey, historicalState.Bytes())

		// Should error with invalid future height
		err := chainStore.Snapshot(1001, streamWriter)
		assert.Error(t, err)

		// Defer order is LIFO
		defer wg.Done()
	}()
	wg.Wait()

	// ----------------
	// OK

	chunks_3 := make(chan io.ReadCloser)
	wg.Add(1)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_3)
		defer streamWriter.Close()
		require.NotNil(t, streamWriter)

		newState := makeSnapshottableState(t, "test-chain-0", 1010)
		chainStore.GetDatabase().Set(stateKey, newState.Bytes())

		err := chainStore.Snapshot(uint64(1010), streamWriter)
		require.NoError(t, err)

		// Defer order is LIFO
		defer wg.Done()
	}()
	wg.Wait()

	// Retrieving chunks should not error
	for reader := range chunks_3 {
		_, err := io.Copy(io.Discard, reader)
		require.NoError(t, err)
		err = reader.Close()
		require.NoError(t, err)
	}
}

func TestMultiplexChainHistoryStoreSnapshot_DataConsistency(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-snapshot-dataconsistency", 1)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainHistoryStore(t, multiplexDb)
	historicalState := makeHistoricalState(t, "test-chain-0")
	chainStore, ok := multiplexStore["test-chain-0"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)

	chainStore.GetDatabase().Set(stateKey, historicalState.Bytes())

	// Uses a WaitGroup to perform assertions sequentially
	// Note, this wait group is necessary to make sure we wait for all
	// assertions in the goroutine before we proceed with tests.
	var wg sync.WaitGroup

	// ----------------
	// Errors
	//
	// - Must error and stop the snapshotting process given faulty chunks writer.
	// - Must error and stop the snapshotting process given failing extension.

	expectedError := fmt.Sprintf(
		"CLIENT PANIC: failing snapshot mutation extension: %s", testPanicMessage)

	chunks_1 := make(chan io.ReadCloser)
	wg.Add(1)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_1)
		defer streamWriter.Close() // also closes chunks_1
		require.NotNil(t, streamWriter)

		newState := makeSnapshottableState(t, "test-chain-0", 1010)
		chainStore.GetDatabase().Set(stateKey, newState.Bytes())

		// Forces a FAILING snapshot mutation extension
		errExtensionInjecter := mx.WithSnapshotMutationExtension(
			mockSnapshotMutationExtension_WithError,
		)
		errExtensionInjecter(chainStore)

		// conditions are fine, except the failing extension
		err := chainStore.Snapshot(uint64(1010), streamWriter)

		// Returning an error means that the snapshotting process is STOPPED!
		assert.Error(t, err, "should stop snapshotting process given failing extension")
		assert.Equal(t, expectedError, err.Error())

		// Defer order is LIFO
		defer wg.Done()
	}()
	wg.Wait()

	// ----------------
	// OK

	chunks_2 := make(chan io.ReadCloser)
	wg.Add(1)
	go func() {
		streamWriter := snapshots.NewStreamWriter(chunks_2)
		defer streamWriter.Close() // also closes chunks_2
		require.NotNil(t, streamWriter)

		newState := makeSnapshottableState(t, "test-chain-0", 1010)
		chainStore.GetDatabase().Set(stateKey, newState.Bytes())

		// Forces a WORKING snapshot mutation extension
		okExtensionInjecter := mx.WithSnapshotMutationExtension(
			mockSnapshotMutationExtension_HashedState,
		)
		okExtensionInjecter(chainStore)

		// Snapshotting state should not produce an error this time around.
		err := chainStore.Snapshot(uint64(1010), streamWriter)

		// Should not error given a working extension
		assert.NoError(t, err)

		// Defer order is LIFO
		defer wg.Done()
	}()
	wg.Wait()

	// Retrieving chunks should not produce errors given a working extension.
	for reader := range chunks_2 {
		_, err := io.Copy(io.Discard, reader)
		require.NoError(t, err)
		err = reader.Close()
		require.NoError(t, err)
	}
}

func TestMultiplexChainHistoryStoreRestore(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-restore", 2)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainHistoryStore(t, multiplexDb)
	newState := makeSnapshottableState(t, "test-chain-0", 1010)
	chainStore, ok := multiplexStore["test-chain-0"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)

	// using 2 different state instances (but same ChainID)
	otherState := makeSnapshottableState(t, "test-chain-0", 500)
	otherState.AppHash = []byte{3, 2, 1} // different before restoration
	otherStore, ok := multiplexStore["test-chain-1"].GetInstance().(*mx.ChainHistoryStore)
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
	_, err = otherStore.Restore(1010, snapshottypes.CurrentFormat, reader)
	require.NoError(t, err)

	// Must have loaded the AppHash from the restored snapshot
	assert.Equal(t, true, bytes.Equal(chainStore.AppHash(), otherStore.AppHash()))

	// Should have reloaded new snapshotted state
	reloadedState, err := otherStore.LoadArchive(stateKey)
	require.NoError(t, err)
	assert.Equal(t, true, bytes.Equal(newState.Bytes(), reloadedState.Bytes()))
}

func TestMultiplexChainHistoryStoreRestore_DataConsistency(t *testing.T) {
	rootDir, multiplexDb := ResetMultiplexDBTestRoot(t, "test-mx-chain-state-store-restore-dataconsistency", 2)
	defer os.RemoveAll(rootDir)

	multiplexStore := makeMultiplexChainHistoryStore(t, multiplexDb)
	newState := makeSnapshottableState(t, "test-chain-0", 1010)
	chainStore, ok := multiplexStore["test-chain-0"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)

	// using 2 different state instances (but same ChainID)
	otherState := makeSnapshottableState(t, "test-chain-0", 500)
	otherState.AppHash = []byte{3, 2, 1} // different before restoration
	otherStore, ok := multiplexStore["test-chain-1"].GetInstance().(*mx.ChainHistoryStore)
	require.Equal(t, true, ok)

	chainStore.GetDatabase().Set(stateKey, newState.Bytes())
	otherStore.GetDatabase().Set(stateKey, otherState.Bytes())

	// Uses a WaitGroup to perform assertions sequentially
	// Note, this wait group is necessary to make sure we wait for all
	// assertions in the goroutine before we proceed with tests.
	var wg sync.WaitGroup

	// ----------------
	// Errors
	//
	// - Must error and stop the restoration process given faulty snapshot.
	// - Must error and stop the restoration process given failing extension.

	expectedError := fmt.Sprintf(
		"CLIENT PANIC: failing snapshot restoration extension: %s", testPanicMessage)

	wg.Add(1)
	go func() {
		// Take a snapshot
		chunks := make(chan io.ReadCloser)
		go func() {
			streamWriter := snapshots.NewStreamWriter(chunks)
			defer streamWriter.Close()
			require.NotNil(t, streamWriter)

			err := chainStore.Snapshot(uint64(1010), streamWriter)
			require.NoError(t, err)
		}()

		reader, err := snapshots.NewStreamReader(chunks)
		require.NoError(t, err)

		// Forces a FAILING snapshot restoration extension
		errExtensionInjecter := mx.WithSnapshotRestoreExtension(
			mockSnapshotRestoreExtension_WithError,
		)
		errExtensionInjecter(otherStore)

		// Should error due to failing snapshot extension
		_, err = otherStore.Restore(1010, snapshottypes.CurrentFormat, reader)

		// Returning an error means that the restoration process is STOPPED!
		assert.Error(t, err, "should stop restoration process given failing extension")
		assert.Equal(t, expectedError, err.Error())

		// Defer order is LIFO
		defer wg.Done()
	}()
	wg.Wait()

	// ----------------
	// OK

	wg.Add(1)
	go func() {
		// Take a snapshot
		chunks := make(chan io.ReadCloser)
		go func() {
			streamWriter := snapshots.NewStreamWriter(chunks)
			defer streamWriter.Close()
			require.NotNil(t, streamWriter)

			err := chainStore.Snapshot(uint64(1010), streamWriter)
			require.NoError(t, err)
		}()

		reader, err := snapshots.NewStreamReader(chunks)
		require.NoError(t, err)

		// Forces a WORKING snapshot restoration extension
		okExtensionInjecter := mx.WithSnapshotRestoreExtension(
			mockSnapshotRestoreExtension_DeepCopy,
		)
		okExtensionInjecter(otherStore)

		// .. and try to restore the snapshot (on different store instance for tests)
		_, err = otherStore.Restore(1010, snapshottypes.CurrentFormat, reader)
		require.NoError(t, err)

		// Must have loaded the AppHash from the restored snapshot
		assert.Equal(t, true, bytes.Equal(chainStore.AppHash(), otherStore.AppHash()))

		// Should have reloaded new snapshotted state
		reloadedState, err := otherStore.LoadArchive(stateKey)
		require.NoError(t, err)
		assert.Equal(t, true, bytes.Equal(newState.Bytes(), reloadedState.Bytes()))

		// Defer order is LIFO
		defer wg.Done()
	}()
	wg.Wait()
}

// ----------------------------------------------------------------------------

func makeMultiplexChainHistoryStore(
	t *testing.T,
	multiplexDb mx.MultiplexDB,
) mx.MultiplexMap[*mx.ChainHistoryStore] {
	t.Helper()

	multiplexChainStore := make(mx.MultiplexMap[*mx.ChainHistoryStore], len(multiplexDb))
	for chainId, db := range multiplexDb {
		multiplexChainStore[chainId] = mx.NewChainInstance(chainId, &mx.ChainHistoryStore{
			ChainID: chainId,
			DBStore: sm.NewDBStore(db, sm.StoreOptions{
				DiscardABCIResponses: false,
				DBKeyLayout:          "v2",
			}).(*sm.DBStore),
		})
	}

	return multiplexChainStore
}

func makeHistoricalState(t *testing.T, chainId string) *mx.HistoricalState {
	t.Helper()

	// Augments a sm.State to form a mx.HistoricalState
	state := makeGenesisState(t, chainId)
	return &mx.HistoricalState{
		State: &state,
		Data:  []byte(`{"account_owner":"Charlie"}`),
	}
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
) *mx.HistoricalState {
	t.Helper()

	state := makeHistoricalState(t, chainId)

	state.LastBlockHeight = setHeight
	state.LastBlockID = types.BlockID{}
	state.LastBlockTime = cmttime.Now()
	state.LastValidators = state.Validators
	state.NextValidators = state.Validators

	return state
}
