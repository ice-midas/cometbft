package client

import (
	"context"

	"github.com/cometbft/cometbft/config"
)

// SyncConfigExtensionFn provides an interface for state-sync config extensions
// that are used in [InjectSyncConfig] to delegate the retrieval of state-sync
// configuration objects to potential extensions.
//
// We provide an example [DefaultSyncConfigExtension] implementation.
type SyncConfigExtensionFn func(
	context.Context,
	*config.StateSyncConfig,
) *config.StateSyncConfig

// SeedConfigExtensionFn provides an interface for seed nodes config extensions
// that are used in [InjectChainSeeds] to delegate the retrieval of seed nodes
// configuration strings to potential extensions.
//
// Note that the seed nodes configuration is a mere `string` which consist of
// a comma-separated list of seed nodes with the format: `id@host:port`.
//
// We provide an example [DefaultSeedConfigExtension] implementation.
type SeedConfigExtensionFn func(
	context.Context,
	string,
) string

// SnapshotMutationExtensionFn provides an interface for state mutation extensions
// that are used in [InjectSnapshotMutation] to delegate the processing of state
// and the mutations of data, to potential extensions.
//
// Note that we provide the *state* instance as a `[]byte` slice here.
//
// We provide an example [DefaultSnapshotMutationExtension] implementation.
type SnapshotMutationExtensionFn func(
	context.Context,
	[]byte,
) []byte

// PrepareProposalExtensionFn provides an interface for transactions mutation
// extensions that are used in [InjectPrepareProposal] to delegate the
// pre-processing of transactions data, to potential extensions.
//
// Note that transactions *may* be discarded by this handler so that they are
// not included in the next block of the network.
//
// Note that we provide the *transactions* as a `[][]byte` slice here.
//
// We provide an example [DefaultPrepareProposalExtension] implementation.
type PrepareProposalExtensionFn func(
	context.Context,
	[][]byte,
) [][]byte

// FinalizeBlockExtensionFn provides an interface for blocks mutation extensions
// that are used in [InjectFinalizeBlock] to delegate the post-processing of
// state transactions data, to potential extensions.
//
// Note that transactions *may* be discarded or updated by this handler so that
// the triggered [abcitypes.Event] contains a *mutated* slice of transactions
// before finalizing the block - and thus use the mutated data.
//
// Note that we provide the *transactions* as a `[][]byte` slice here.
//
// We provide an example [DefaultFinalizeBlockExtension] implementation.
type FinalizeBlockExtensionFn func(
	context.Context,
	[][]byte,
) [][]byte
