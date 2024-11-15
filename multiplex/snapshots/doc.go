/*
The `snapshots` package implements automatic support for CometBFT state sync
bootstrapping of nodes. State sync allows a new node joining a network to
simply fetch a recent snapshot of the application state instead of fetching
and applying all historical blocks. This can reduce the time needed to join the
network by several orders of magnitude (e.g. weeks to minutes).

# StateSnapshotter

The [Manager] implementation uses only *one* snapshotter: [StateSnapshotter]
which shall provide added methods `GetStateMachine()` and `AppHash()`.

This snapshotter executes upon **Commit()** of a block - i.e. after finalizing
blocks - in cosmos-sdk, this type of snapshotter is called a commitment
snapshotter because it is used to store block header information used in Commit.

The [ChunkWriter] and [ChunkReader] implementations are unchanged from the
original `cosmos-sdk` snapshots manager implementation.

The [Store] implementation is also unchanged from the original `cosmos-sdk`
snapshots manager implementation and serves as a storage layer using the
filesystem to store snapshots metadata and snapshot chunks.

The [WriterCloser], [StreamReader] and [StreamWriter] imeplementations are
as well, unchanged from the original `cosmos-sdk` snapshots manager
implementation. These are used to enable *streaming* snapshots data.

# Interface

The [Manager] implementation makes abstraction of the underlying *snapshotter*
instance being used by adding the [StateSnapshotter] interface.

An external implementation for this interface must be implemented. Following
implementation contract applies:

- [StateSnapshotter#Snapshot]: Should take a snapshot.
- [StateSnapshotter#Restore]: Should restore a snapshot.
- [StateSnapshotter#GetStateMachine]: Should return a copy of the [sm.State].
- [StateSnapshotter#AppHash]: Should return the `AppHash` from loaded state.

We shall provide this implementation with `ChainHistoryStore` in `multiplex`.

## Testing

You can test the chunks stream, snapshots manager and snapshotting features
using the following unit test suite:

```bash
go test github.com/ice-blockchain/cometbft/multiplex/snapshots -test.v -count=1
```

Note that this test suite is apart from the `snapshots` package and implemented
in a `snapshots_test` package instead.
*/
package snapshots
