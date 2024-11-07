package multiplex

import (
	"errors"
	"fmt"
	"io"
	"time"

	protoio "github.com/cosmos/gogoproto/io"
	"github.com/cosmos/gogoproto/proto"

	cmtstate "github.com/cometbft/cometbft/api/cometbft/state/v1"
	cmtos "github.com/cometbft/cometbft/internal/os"
	sm "github.com/cometbft/cometbft/state"

	"github.com/cometbft/cometbft/multiplex/client"
	"github.com/cometbft/cometbft/multiplex/snapshots"
	snapshottypes "github.com/cometbft/cometbft/multiplex/snapshots/types"
)

// database keys.
var (
	// IMPORTANT: Do not modify this key because it is used to store the
	// current state machine payload (bytes). see state/state.go.
	//
	// We reuse this same key to be able to reload an entire state machine.
	stateKey = []byte("stateKey")

	// Note: we use a separate key space for the genesis doc and doc hashes
	// to prevent mixing both storages as mxGenesisDoc holds GenesisDocSet.
	genesisDocKey     = []byte("mxGenesisDoc")
	genesisDocHashKey = []byte("mxGenesisDocHash")
)

// ----------------------------------------------------------------------------
// ChainStateStore
//
// ChainStateStore embeds a [sm.DBStore] pointer and adds a ChainID. This store
// implementation is compatible with [snapshots.StateSnapshotter] such that
// the full state instance may be *restored* (state-sync) from snapshot files.
//
// Note that this structure embeds a [sm.DBStore] which provides methods for
// the implementation of an interface [sm.Store]. This structure overwrites the
// Load method to load the state from a database instance.
type ChainStateStore struct {
	ChainID     string
	*sm.DBStore // embeds an implementation of [sm.Store]

	// Used for user address discovery by ChainID
	chainRegistry ChainRegistry

	// Extensions / Hooks
	snapshotMutationHook client.SnapshotMutationExtensionFn
	snapshotRestoreHook  client.SnapshotRestoreExtensionFn
}

// MultiplexChainStore maps ChainIDs to state store instances
type MultiplexChainStore map[string]*ChainStateStore

// State machine compatibility check
var _ sm.Store = (*ChainStateStore)(nil)
var _ snapshots.StateSnapshotter = (*ChainStateStore)(nil)

// GetStateMachine returns the latest application state (state machine).
//
// GetStateMachine implements [snapshottypes.StateSnapshotter]
func (store ChainStateStore) GetStateMachine() (sm.State, error) {
	return store.Load()
}

// Load loads the State from the database using the stateKey.
//
// Load implements [sm.Store]
func (store ChainStateStore) Load() (sm.State, error) {
	return store.loadState(stateKey)
}

// SetChainRegistry registers a [ChainRegistry] which is used to find
// user addresses by ChainID.
func (store ChainStateStore) SetChainRegistry(reg ChainRegistry) {
	store.chainRegistry = reg
}

// GetUserAddress returns a hexadecimal representation of the user address
// corresponding to the current ChainID, given it exists. Otherwise it will
// return an empty string.
func (store ChainStateStore) GetUserAddress() string {
	if store.chainRegistry == nil {
		return ""
	}

	userAddress, err := store.chainRegistry.GetAddress(store.ChainID)
	if err != nil {
		return ""
	}

	return userAddress
}

// GetChainID returns the current ChainID as a string.
func (store ChainStateStore) GetChainID() string {
	return store.ChainID
}

// Commit updates the state instance in the database.
//
// This method is called as part of the process of restoration of snapshots
// which is issued in a separate goroutine. The state machine instance in
// the database is updated to contain the snapshot's state instance.
//
// CAUTION: This method overwrites the state machine instance as stored
// in the state database, using stateKey.
func (store ChainStateStore) Commit(state sm.State) error {
	// (1): Retrieves the current state from DB
	oldState, err := store.GetStateMachine()
	if err != nil {
		return err
	}

	// (2): Validate compatibility of new State
	if oldState.ChainID != state.ChainID {
		return fmt.Errorf(
			"cannot commit state from other chain; expected %v, got %v",
			state.ChainID, oldState.ChainID,
		)
	}

	// (3): Commit the new State to DB
	if err := store.GetDatabase().Set(stateKey, state.Bytes()); err != nil {
		return err
	}

	return nil
}

// AppHash returns the application hash as available from the state machine.
//
// AppHash implements [snapshottypes.StateSnapshotter]
func (store ChainStateStore) AppHash() []byte {
	// Load the state machine
	sm, err := store.GetStateMachine()

	if err != nil {
		// TODO(midas): TBI the occurence of this error.
		cmtos.Exit(fmt.Sprintf(`could not load state machine for ChainID %s: %v\n`,
			store.ChainID,
			err.Error(),
		))
	}

	return sm.AppHash
}

// loadState loads the State from the database given a key.
func (store ChainStateStore) loadState(key []byte) (state sm.State, err error) {
	start := time.Now()
	buf, err := store.GetDatabase().Get(key)
	if err != nil {
		return state, err
	}

	defer addTimeSample(store.StoreOptions.Metrics.StoreAccessDurationSeconds.With("method", "load"), start)()
	return store.loadStateFromPayload(buf)
}

// loadStateFromPayload takes a bytes payload and unmarshals it to a [sm.State].
func (store ChainStateStore) loadStateFromPayload(payload []byte) (state sm.State, err error) {
	if len(payload) == 0 {
		return sm.State{}, nil
	}

	sp := new(cmtstate.State)

	err = proto.Unmarshal(payload, sp)
	if err != nil {
		// DATA HAS BEEN CORRUPTED OR THE SPEC HAS CHANGED
		cmtos.Exit(fmt.Sprintf(`LoadState: Data has been corrupted or its spec has changed:
		%v\n`, err))
	}

	sm, err := sm.FromProto(sp)
	if err != nil {
		return state, err
	}
	return *sm, nil
}

// ----------------------------------------------------------------------------
// ChainStateStore implements snapshottypes.Snapshotter

// The snapshot output for a given format must be identical across nodes such
// that chunks from different sources fit together. If the output for a given
// format changes (at the byte level), the snapshot format must be bumped.
//
// State is serialized as a stream of SnapshotItem Protobuf messages which
// contain a bytes payload of the state store. As such, each SnapshotItem
// represents a state instance and snapshots contain only one item.
//
// It is safe to call the [client.InjectSnapshotMutation] extension because
// the caller is inside a *separate* goroutine, spawned for the process of
// creation of snapshots which is run in a separate goroutine.
//
// Snapshot implements [snapshottypes.StateSnapshotter]
func (store ChainStateStore) Snapshot(
	height uint64,
	protoWriter protoio.Writer,
) error {
	// Verify snapshot height (logic errors)
	if height == 0 {
		return errors.New("cannot snapshot height 0")
	}

	// Load latest state for further verifications
	latestState, err := store.Load()
	if err != nil {
		return fmt.Errorf("could not load state to create snapshot: %w", err)
	}

	// Can only snapshot "now" / latest state
	if height > uint64(latestState.LastBlockHeight) {
		return fmt.Errorf("cannot snapshot future height %v", height)
	}

	// Prepare the contract for state mutation extensions
	var mutatedStateBytes []byte

	// Executes snapshot mutation extension and catches potential panics to
	// ensure data consistency, also making a compromise on availability.
	//
	// Catch recovered errors from snapshot mutation extension and stop.
	// When a snapshot mutation extension fails, the data cannot be snapshotted
	// because the extension determines the format of the mutated state machine.
	mutatedStateBytes, err = store.runSnapshotMutationHook(latestState)
	if err != nil {
		return fmt.Errorf(
			"CLIENT PANIC: failing snapshot mutation extension: %w", err)
	}

	// Reaching the following block means that the snapshot mutation extension
	// mutated the snapshot state correctly and that no errors happened during
	// the mutation process.

	// State is serialized as a stream of SnapshotItem Protobuf
	// messages which contain a bytes payload of the state store.
	// As such, each SnapshotItem represents a full state instance.
	err = func() error {
		err = protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
			Item: &snapshottypes.SnapshotItem_Store{
				Store: &snapshottypes.SnapshotStoreItem{
					Payload: mutatedStateBytes,
				},
			},
		})

		if err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		return err
	}

	return nil
}

// Restore restores [snapshottypes.SnapshotItem] instances into a bytes payload
// and commits the new state if available.
//
// Restore implements [snapshottypes.StateSnapshotter]
func (store ChainStateStore) Restore(
	height uint64,
	format uint32,
	protoReader protoio.Reader,
) (snapshottypes.SnapshotItem, error) {
	// We restore as many items as there are for a specific snapshot
	var snapshotItem snapshottypes.SnapshotItem
	var restoredStateBytes []byte

	// Reads the next snapshot item until it finds EOF or an error occurs
RESTORE_LOOP:
	for {
		snapshotItem = snapshottypes.SnapshotItem{}
		err := protoReader.ReadMsg(&snapshotItem)

		// If we reached EOF, we are done restoring
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf("could not read snapshot item message: %w", err)
		}

		switch item := snapshotItem.Item.(type) {
		case *snapshottypes.SnapshotItem_Store:
			restoredStateBytes = item.Store.Payload
			if restoredStateBytes == nil || len(restoredStateBytes) == 0 {
				return snapshottypes.SnapshotItem{}, errors.New("found empty snapshot item payload")
			}

			// store.logger.Debug("restoring snapshot",
			// 	"chain_id", store.ChainID,
			// 	"height", height,
			// 	"size", len(restoredStateBytes),
			// )

		default:
			break RESTORE_LOOP
		}
	}

	if restoredStateBytes != nil {
		// Executes snapshot restoration extension and catches potential panics to
		// ensure data consistency, also making a compromise on availability.
		//
		// Catch recovered errors from snapshot restore extension and stop.
		// When a snapshot restore extension fails, the data cannot be restored
		// because the extension determines the format of the mutated state machine.
		restoredStateBytes, err := store.runSnapshotRestoreHook(restoredStateBytes)
		if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf(
				"CLIENT PANIC: failing snapshot restoration extension: %w", err)
		}

		// Create a [sm.State] instance from snapshot item data
		newState, err := store.loadStateFromPayload(restoredStateBytes)
		if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf(
				"could not load snapshot data: %w", err)
		}

		// Commit the restoration process, this updates the database
		if err = store.Commit(newState); err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf(
				"could not commit restored state: %w", err)
		}
	}

	// Re-Load the newly commited/restored state instance
	_, err := store.Load()
	return snapshotItem, err
}

// runSnapshotMutationHook executes a snapshot mutation extension. This method
// uses the snapshotMutationHook on the instance if available, or otherwise it
// uses the extension configured as the *default*
//
// Executes snapshot mutation extension and catches potential panics to ensure
// data consistency, also making a compromise on availability.
// When a snapshot mutation extension fails, the data cannot be snapshotted
// because the extension determines the format of the mutated state machine.
//
// See also: [GetSnapshotMutationExtension], [WithSnapshotMutationExtension].
func (store ChainStateStore) runSnapshotMutationHook(latestState sm.State) ([]byte, error) {
	// Prepare the contract for state mutation extensions
	var mutatedStateBytes []byte
	var snapshotMutationHook client.SnapshotMutationExtensionFn

	// Uses the default extension or the one configured
	snapshotMutationHook = GetSnapshotMutationExtension()
	if store.snapshotMutationHook != nil {
		snapshotMutationHook = store.snapshotMutationHook
	}

	// Executes snapshot mutation extension and catch potential panics to
	// ensure data consistency, also making a compromise on availability.
	//
	// When a snapshot mutation extension fails, the data cannot be snapshotted
	// because the extension determines the format of the mutated state machine.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a state mutation extension. This recovery ensures *data consistency*
		// in case of failing snapshot mutation extensions, and thus breaks the
		// snapshotting process until the state mutation extension is fine.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in state mutation extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. deep-copy the sm.State
		// See `multiplex/client.go` to use a custom state mutation extension.
		// See also [WithSnapshotMutationExtension].
		mutatedStateBytes = client.InjectSnapshotMutation(
			store.GetChainID(),
			latestState.Bytes(),
			snapshotMutationHook,
		)

		return err
	}()

	// Catch recovered errors from snapshot mutation extension and stop.
	// When a snapshot mutation extension fails, the data cannot be snapshotted
	// because the extension determines the format of the mutated state machine.
	if err != nil {
		return []byte{}, err
	}

	// No errors happened, we can safely use the mutated state instance
	return mutatedStateBytes, nil
}

// runSnapshotRestoreHook executes a snapshot restoration extension. This
// method uses the snapshotRestoreHook on the instance if available, or
// otherwise it uses the extension configured as the *default*.
//
// Executes snapshot restoration extension and catches potential panics
// to ensure data consistency, also making a compromise on availability.
// When a snapshot restore extension fails, the data cannot be restored
// because the extension determines the format of the restored state machine.
//
// See also: [GetSnapshotRestoreExtension], [WithSnapshotRestoreExtension].
func (store ChainStateStore) runSnapshotRestoreHook(stateBytes []byte) ([]byte, error) {
	// Prepare the contract for state restoration extensions
	var restoredStateBytes []byte
	var snapshotRestoreHook client.SnapshotRestoreExtensionFn

	// Uses the default extension or the one configured
	snapshotRestoreHook = GetSnapshotRestoreExtension()
	if store.snapshotRestoreHook != nil {
		snapshotRestoreHook = store.snapshotRestoreHook
	}

	// Executes snapshot restoration extension and catch potential panics
	// to ensure data consistency, making a compromise on availability.
	//
	// When a snapshot restore extension fails, the data cannot be restored
	// because the extension determines the format of the mutated state machine.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a state restoration extension. This recovery ensures *data consistency*
		// in case of failing snapshot restoration extensions, and thus breaks the
		// snapshotting process until the state restoration extension is fine.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in state restoration extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. deep-copy the bytes slice
		// see `multiplex/client.go` to use a custom state restoration extension.
		// See also [WithSnapshotRestoreExtension].
		restoredStateBytes = client.InjectSnapshotRestore(
			store.GetChainID(),
			stateBytes,
			snapshotRestoreHook,
		)

		return err
	}()

	// Catch recovered errors from snapshot restoration extension and stop.
	// When a snapshot restoration extension fails, the data cannot be restored
	// because the extension determines the format of the restored state machine.
	if err != nil {
		return []byte{}, err
	}

	// No errors happened, we can safely use the restored bytes slice
	return restoredStateBytes, nil
}

// ----------------------------------------------------------------------------
// ChainStateStore option helpers

// WithSnapshotMutationExtension is an option helper that allows you to
// overwrite the default (deep-copying) snapshot mutation extension.
func WithSnapshotMutationExtension(
	extensionFn client.SnapshotMutationExtensionFn,
) func(*ChainStateStore) {
	return func(store *ChainStateStore) {
		store.snapshotMutationHook = extensionFn
	}
}

// WithSnapshotRestoreExtension is an option helper that allows you to
// overwrite the default (deep-copying) snapshot restoration extension.
func WithSnapshotRestoreExtension(
	extensionFn client.SnapshotRestoreExtensionFn,
) func(*ChainStateStore) {
	return func(store *ChainStateStore) {
		store.snapshotRestoreHook = extensionFn
	}
}
