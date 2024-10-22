# SnapsApp ABCI

The `snapsapp` package implements a multi-network ABCI application that enables
state snapshotting and bootstrapping nodes with state-sync.

This document describes the `multiplex/snapsapp` implementation of the ABCI
application interface, for more information on CometBFT state sync in general see:

* [CometBFT State Sync for Developers](https://medium.com/cometbft/cometbft-core-state-sync-for-developers-70a96ba3ee35)
* [ABCI State Sync](https://docs.cometbft.com/v1.0/explanation/core/state-sync)
* [ABCI State Sync Methods](https://docs.cometbft.com/v1.0/spec/abci/abci++_basic_concepts#state-sync-methods)
* [Cosmos-SDK State Sync Snapshotting](https://github.com/cosmos/cosmos-sdk/blob/release/v0.50.x/store/snapshots/README.md)

## SnapsApp

The [SnapsApp] structure is configured via the following instances:

- A `config.Config` contains a replication `Strategy` and `SnapshotOptions`.
- A `multiplex.ChainRegistry` contains the multiplex chain registry config.
- A `multiplex.MultiplexFS` contains filesystem paths per replicated chain.
- A `multiplex.MultiplexChainStore` contains the `ChainStateStore` objects.
- A `map[string]*snapshots.Manager` contains the snapshot manager per replicated chain.

When the SnapsApp is created, the replicated chains configuration is read from
the chain registry interface, and snapshots managers are created for each of
the replicated chains, then mapped to their respective `ChainID`.

The `ChainStateStore` structure, that is passed at creation, is expected to
satisfy the `snapshots.StateSnapshotter` interface by implementing: Snapshot(),
Restore(), GetStateMachine() and AppHash().

The SnapsApp ABCI application delegates to the snapshots manager and the
snapshotter implementation for the underlying processes of taking snapshots
and restoring them.

### ABCI Interface

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

## Testing

You can test the ABCI methods using the following unit test suite:

```bash
go test github.com/cometbft/cometbft/multiplex/snapsapp -test.v -count=1
```

Note that this test suite is apart from the `multiplex` package and implemented
in a `snapsapp_test` package instead.
