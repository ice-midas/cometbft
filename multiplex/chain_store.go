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
	cmtlog "github.com/cometbft/cometbft/libs/log"
	sm "github.com/cometbft/cometbft/state"

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
)

// ----------------------------------------------------------------------------
// ChainStateStore
//
// ChainStateStore embeds a [sm.DBStore] pointer and adds a ChainID. This store
// implementation is compatible with [snapshots.CommitSnapshotter] such that
// the full state instance may be *restored* (state-sync) from snapshot files.
//
// Note that this structure embeds a [sm.DBStore] which provides methods for
// the implementation of an interface [sm.Store]. This structure overwrites the
// Load method to load the state from a database instance.
type ChainStateStore struct {
	ChainID     string
	*sm.DBStore // embeds an implementation of [sm.Store]

	// A logger is used during asynchronous snapshot restoration
	logger cmtlog.Logger
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

// Commit updates the state instance in the database.
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

	// State is serialized as a stream of SnapshotItem Protobuf
	// messages which contain a bytes payload of the state store.
	// As such, each SnapshotItem represents a full state instance.
	err = func() error {
		err = protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
			Item: &snapshottypes.SnapshotItem_Store{
				Store: &snapshottypes.SnapshotStoreItem{
					Payload: latestState.Bytes(),
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
	var stateBytes []byte

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
			stateBytes = item.Store.Payload
			if stateBytes == nil || len(stateBytes) == 0 {
				return snapshottypes.SnapshotItem{}, errors.New("found empty snapshot item payload")
			}

			store.logger.Debug("restoring snapshot",
				"chain_id", store.ChainID,
				"height", height,
				"size", len(stateBytes),
			)

		default:
			break RESTORE_LOOP
		}
	}

	if stateBytes != nil {
		// Create a [sm.State] instance from snapshot item data
		newState, err := store.loadStateFromPayload(stateBytes)
		if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf("could not load snapshot data: %w", err)
		}

		// Commit the restoration process, this updates the database
		if err = store.Commit(newState); err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf("could not commit restored state: %w", err)
		}
	}

	// Re-Load the newly commited/restored state instance
	_, err := store.Load()
	return snapshotItem, err
}

// ----------------------------------------------------------------------------
// Providers

// NewMultiplexChainStore creates multiple ChainStateStore instances using
// the database multiplex dbm.DB instances and the state machine DBStore.
func NewMultiplexChainStore(
	multiplex MultiplexDB,
	options sm.StoreOptions,
	logger cmtlog.Logger,
) (mStore MultiplexChainStore) {
	mStore = make(MultiplexChainStore, len(multiplex))
	for _, db := range multiplex {
		mStore[db.ChainID] = &ChainStateStore{
			ChainID: db.ChainID,
			DBStore: sm.NewDBStore(db, options).(*sm.DBStore),

			// A logger is used during asynchronous snapshot restoration
			logger: logger,
		}
	}

	return mStore
}

// NewChainStateStore tries to find a state store in the store multiplex using its ChainID
func NewChainStateStore(
	multiplex MultiplexChainStore,
	chainId string,
) (*ChainStateStore, error) {
	if chainStateStore, ok := multiplex[chainId]; ok {
		return chainStateStore, nil
	}

	return nil, fmt.Errorf("could not find chain state store in multiplex using ChainID %s", chainId)
}
