package multiplex

import (
	"github.com/cometbft/cometbft/multiplex/client"
)

// GetSyncConfigExtension returns the active configuration extension for state-sync.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom configuration extension.
func GetSyncConfigExtension() client.SyncConfigExtensionFn {
	return client.DefaultSyncConfigExtension
}

// GetSeedConfigExtension returns the active configuration extension for seed nodes.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom configuration extension.
func GetSeedConfigExtension() client.SeedConfigExtensionFn {
	return client.DefaultSeedConfigExtension
}

// GetSnapshotMutationExtension returns the active data extension for state mutations.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data processing unit, or extension.
func GetSnapshotMutationExtension() client.SnapshotMutationExtensionFn {
	return client.DefaultSnapshotMutationExtension
}

// GetSnapshotRestoreExtension returns the active data extension for state restorations.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data processing unit, or extension.
func GetSnapshotRestoreExtension() client.SnapshotRestoreExtensionFn {
	return client.DefaultSnapshotRestoreExtension
}
