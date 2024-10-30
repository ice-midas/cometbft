package client

import (
	"context"

	"github.com/cometbft/cometbft/config"
)

// ----------------------------------------------------------------------------
// Extensions
//
// We hereby provide *example* implementations for the configuration extensions
// that may be used to *inject configuration* from the client of this library.

// DefaultSyncConfigExtension is an example implementation for the state-sync
// config extension [SyncConfigExtensionFn]. A state-sync config extension may
// be used to *overwrite* state-sync configuration for a particular network.
//
// Fields that may be provided from an external process may include:
// - `StateSyncConfig.TrustPeriod`
// - `StateSyncConfig.TrustHeight`
// - `StateSyncConfig.TrustHash`
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
func DefaultSyncConfigExtension(
	ctx context.Context,
	baseSyncConf *config.StateSyncConfig,
) *config.StateSyncConfig {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	// Deep-copy the StateSyncConfig object
	nextStateSyncConfig := &config.StateSyncConfig{
		Enable:              baseSyncConf.Enable,              // bool
		TempDir:             baseSyncConf.TempDir,             // string
		RPCServers:          baseSyncConf.RPCServers,          // []string
		TrustPeriod:         baseSyncConf.TrustPeriod,         // time.Duration
		TrustHeight:         baseSyncConf.TrustHeight,         // int64
		TrustHash:           baseSyncConf.TrustHash,           // string
		DiscoveryTime:       baseSyncConf.DiscoveryTime,       // time.Duration
		ChunkRequestTimeout: baseSyncConf.ChunkRequestTimeout, // time.Duration
		ChunkFetchers:       baseSyncConf.ChunkFetchers,       // int32
	}

	return nextStateSyncConfig
}

// DefaultSeedConfigExtension is an example implementation for the seed nodes
// extension [SeedConfigExtensionFn]. A seed nodes config extension may
// be used to *overwrite* seed nodes configuration for a particular network.
//
// i.e. An extension may be implemented to retrieve seed nodes from a custom
// remote server, or to mutate the available baseSeeds from config.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
func DefaultSeedConfigExtension(
	ctx context.Context,
	baseSeeds string,
) string {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextSeeds := baseSeeds[:]
	return nextSeeds
}

// DefaultSnapshotMutationExtension is an example implementation for the state
// mutation extension [SnapshotMutationExtensionFn]. A state mutation extension
// may be used to *process* or *mutate* state raw bytes for a particular network.
//
// i.e. An extension may be implemented to store a full state machine's bytes
// representation in a separate database instance.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
func DefaultSnapshotMutationExtension(
	ctx context.Context,
	baseState []byte,
) []byte {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextState := baseState[:]
	return nextState
}

// DefaultPrepareProposalExtension is an example implementation for the
// transactions mutation extension [PrepareProposalExtensionFn]. A transactions
// mutation extension may be used to *pre-process* or *mutate* transactions raw
// bytes before they are added to a proposal for a particular network.
//
// i.e. An extension may be implemented to store transaction data bytes
// representation in a separate database instance.
//
// Note that we inject `ChainID` in the Context before calling the proposed
// extension callback, this example does not make use of it.
func DefaultPrepareProposalExtension(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	// e.g. You may interpret/use the ChainID in extensions.
	//
	// chainId := ctx.Value("ChainID").(string)

	nextTransactions := baseTransactions[:]
	return nextTransactions
}

// DefaultFinalizeBlockExtension is an example implementation for the
// transactions mutation extension [FinalizeBlockExtensionFn]. A transactions
// mutation extension may be used to *post-process* or *mutate* transactions
// raw bytes as they are added to a finalize block for a particular network.
//
// i.e. An extension may be implemented to store transaction data bytes
// representation in a separate database instance.
//
// Note that we inject `ChainID` in the Context before calling the proposed
// extension callback, this example does not make use of it.
func DefaultFinalizeBlockExtension(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	// e.g. You may interpret/use the ChainID in extensions.
	//
	// chainId := ctx.Value("ChainID").(string)

	nextTransactions := baseTransactions[:]
	return nextTransactions
}
