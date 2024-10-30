/*
The `snapsapp` package implements a multi-network ABCI application that enables
state snapshotting and bootstrapping nodes with state-sync.

# Application

The [SnapsApp] structure is configured via the following instances:

- A `Reactor` contains an implementation to retrieve networks and state stores.
- A `config.SnapshotOptions` contains the snapshot options for the application.
- A `map[string]*snapshots.Manager` contains the snapshot manager per replicated chain.

When the SnapsApp is created, the replicated chains configuration is read from
the reactor interface, and snapshots managers are created for each of
the replicated chains, then mapped to their respective `ChainID`.

The `ChainStateStore` structure, that is retrieve from the reactor, is expected to
satisfy the `snapshots.StateSnapshotter` interface by implementing: Snapshot(),
Restore(), GetStateMachine() and AppHash().

The SnapsApp ABCI application delegates to the snapshots manager and the
snapshotter implementation for the underlying processes of taking snapshots
and restoring them.

# ABCI

The most prominent methods implemented with the [SnapsApp] ABCI application
include, but are not limited to:

- [SnapsApp#InitChain]: InitChain initializes the application's state.
- [SnapsApp#Info]: Info returns information about the application.
- [SnapsApp#Commit]: Commit must determine whether to create a snapshot or not.
- [SnapsApp#ListSnapshots]: ListSnapshots must list recent snapshots.
- [SnapsApp#OfferSnapshot]: OfferSnapshot must parses metadata and starts downloading chunks.
- [SnapsApp#LoadSnapshotChunk]: LoadSnapshotChunk must load a snapshot chunk from filesystem.
- [SnapsApp#ApplySnapshotChunk]: ApplySnapshotChunk applies snapshot chunks sequentially.

We also provide implementations for all other *required* methods, including
for `PrepareProposal`, `ProcessProposal`, `FinalizeBlock` and `Commit`.

# Testing

You can test the ABCI methods using the following unit test suite:

```bash
go test github.com/cometbft/cometbft/multiplex/snapsapp -test.v -count=1
```

Note that this test suite is apart from the `snapsapp` package and implemented
in a `snapsapp_test` package instead.
*/
package snapsapp
