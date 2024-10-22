package snapshots

import (
	protoio "github.com/cosmos/gogoproto/io"

	"github.com/cometbft/cometbft/multiplex/snapshots/types"
	sm "github.com/cometbft/cometbft/state"
)

// StateSnapshotter defines an API for creating and restoring snapshots of the
// commitment state.
type StateSnapshotter interface {
	// Snapshot writes a snapshot of the commitment state at the given version.
	Snapshot(version uint64, protoWriter protoio.Writer) error

	// Restore restores the commitment state from the snapshot reader.
	Restore(version uint64, format uint32, protoReader protoio.Reader) (types.SnapshotItem, error)

	// GetStateMachine should return the latest application state (state machine).
	GetStateMachine() (sm.State, error)

	// AppHash should return the application hash from the state machine.
	AppHash() []byte
}
