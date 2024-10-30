/*
This package provides an example implementation of a client for the multiplex
library. Extensions can be implemented in this client or using the interfaces.

# Extensions

We define the rules for *configuration extensions* and *data extensions*,
which may be used to get custom configuration objects and to process- or mutate
data using a custom client business logic, e.g. which involve calls to remote
servers, or which stores data in a separate database, etc.

# Interfaces

  - [SyncConfigExtensionFn]: Provides custom state-sync configuration values.
  - [SeedConfigExtensionFn]: Provides custom seed nodes configuration values.
  - [SnapshotMutationExtensionFn]: Provides custom processing units for snapshots data.
  - [PrepareProposalExtensionFn]: Provides custom processing units for transactions data.
  - [FinalizeBlockExtensionFn]: Provides custom processing units for blocks data.

We provide several example implementations that basically just *deep-copy* the
input. Obviously, if you are developing a custom extension, you would do more
than just deep-copy input objects.

An example for [SyncConfigExtensionFn] is: [DefaultSyncConfigExtension]
An example for [SeedConfigExtensionFn] is: [DefaultSeedConfigExtension]
An example for [SnapshotMutationExtensionFn] is: [DefaultSnapshotMutationExtension]
An example for [PrepareProposalExtensionFn] is: [DefaultPrepareProposalExtension]
An example for [FinalizeBlockExtensionFn] is: [DefaultFinalizeBlockExtension]

# Custom extensions

You may implement other extensions and use them by modifying the source code
at `multiplex/client.go`. This file is present only for this purpose, thus if
the extension you are developing should *become the default*, you may as well
just overwrite the `Default..Extension` method with your custom business logic.
*/
package client
