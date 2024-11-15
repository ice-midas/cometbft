package multiplex

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cometbft/cometbft/config"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/light"
	"github.com/cometbft/cometbft/multiplex/snapshots"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/statesync"
	bs "github.com/cometbft/cometbft/store"
)

// -----------------------------------------------------------------------------
// Archive

// The Archive implementation takes care of configuring state-sync for a nodes
// multiplex' historical data.
//
// IMPORTANT:
// This implementation sets up a *read-only* node for the supported networks.
// Historical nodes *do not* participate in consensus of the underlying network
// but are used to constantly download the latest snapshot data.
//
// The [Archive] structure implements [snapsapp.Reactor]
type Archive struct {
	p2p.BaseReactor // BaseService + p2p.Switch

	// Node configuration
	nodeKey    *p2p.NodeKey
	nodeConfig *config.Config
	userConfig *config.MultiplexConfig
	abciClient proxy.ChainConns

	// Filesystem and DB
	storagePaths   MultiplexFS
	stateDatabases MultiplexDB
	blockDatabases MultiplexDB

	// Node reactors (services)
	blockStores       MultiplexMap[*bs.BlockStore]
	stateSyncReactors MultiplexMap[*statesync.Reactor]

	// Networks information
	chainRegistry ChainRegistry
	networks      []string
	//nodeInfo      HistoricalNodeInfo

	// Internal
	logger cmtlog.Logger
}

// NewArchive creates a new historical node, or [Archive] instance.
func NewArchive(
	nodeKey *p2p.NodeKey,
	nodeCfg *config.Config,
	logger cmtlog.Logger,
	chainRegistry ChainRegistry,
) *Archive {
	a := &Archive{
		// Provides the ChainRegistry interface
		chainRegistry: chainRegistry,

		// Provides node information and config
		nodeKey:    nodeKey,
		nodeConfig: nodeCfg,
		userConfig: &nodeCfg.MultiplexConfig,

		// Provides an *ordered* slice of ChainID
		networks: chainRegistry.GetChains(),

		// Allocations
		blockStores:       MultiplexMap[*bs.BlockStore]{},
		stateSyncReactors: MultiplexMap[*statesync.Reactor]{},

		// Internals
		logger: logger,
	}
	a.BaseReactor = *p2p.NewBaseReactor("Archive", a)

	return a
}

// ----------------------------------------------------------------------------
// Archive public implementation

// GetNodeConfig returns a [config.Config] instance.
func (a *Archive) GetNodeConfig() *config.Config {
	return a.nodeConfig
}

// GetMultiplexConfig returns a [config.MultiplexConfig] instance.
func (a *Archive) GetMultiplexConfig() *config.MultiplexConfig {
	return a.userConfig
}

// GetStoragePaths returns a [MultiplexFS] instance.
//
// GetStoragePaths implements [snapsapp.Reactor].
func (a *Archive) GetStoragePaths() map[string]string {
	return a.storagePaths
}

// GetNodeKey returns the [p2p.NodeKey] instance.
func (a *Archive) GetNodeKey() *p2p.NodeKey {
	return a.nodeKey
}

// GetChainRegistry returns a [ChainRegistry] instance.
func (a *Archive) GetChainRegistry() ChainRegistry {
	return a.chainRegistry
}

// GetStateSyncReactors returns a [MultiplexMap[*statesync.Reactor]] instance.
func (a *Archive) GetStateSyncReactors() MultiplexMap[*statesync.Reactor] {
	return a.stateSyncReactors
}

// GetNetworks returns an ordered slice of ChainID values.
//
// GetNetworks implements [snapsapp.Reactor].
func (a *Archive) GetNetworks() []string {
	return a.networks
}

// HasNetwork returns true if the ChainID can be found
//
// HasNetwork implements [snapsapp.Reactor].
func (a *Archive) HasNetwork(chainId string) bool {
	return slices.Contains(a.networks, chainId)
}

// GetStateStore returns a [snapshots.StateSnapshotter].
//
// GetStateStore implements [snapsapp.Reactor].
func (a *Archive) GetStateStore(chainId string) snapshots.StateSnapshotter {
	// Initialize a ChainHistoryStore (snapshottable)
	dbKeyLayoutVersion := a.nodeConfig.Storage.ExperimentalKeyLayout
	stateStore := &ChainHistoryStore{
		ChainID: chainId,
		DBStore: sm.NewDBStore(a.stateDatabases[chainId], sm.StoreOptions{
			DiscardABCIResponses: false,
			DBKeyLayout:          dbKeyLayoutVersion,
		}).(*sm.DBStore),
	}

	return stateStore
}

// SetStoragePaths sets a custom [MultiplexFS] map of storage paths.
func (a *Archive) SetStoragePaths(fs MultiplexFS) {
	a.storagePaths = fs
}

// SetABCIClient sets a custom [proxy.ChainConns] ABCI client.
// Note that this method is only used in tests for now.
func (a *Archive) SetABCIClient(abciClient proxy.ChainConns) {
	a.abciClient = abciClient
}

// OnStart initializes a [MultiplexFS] and [MultiplexDB] for this archive
// instance. The database multiplex is used to store historical data.
func (a *Archive) OnStart() error {
	// 1) MultiplexFS
	//
	// Initialize filesystem directory structure
	multiplexFS, err := NewMultiplexFS(a.nodeConfig)
	if err != nil {
		return err
	}

	// Update the internal storagePaths
	a.SetStoragePaths(multiplexFS)

	// 2) MultiplexDB
	//
	// Create state databases per each network
	a.stateDatabases, err = NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "state", Config: a.nodeConfig},
	})
	if err != nil {
		return err
	}

	// 3) BlockStores
	//
	// Create a block store database per each network. This is used to store
	// commit information during the state-sync process.
	a.blockDatabases, err = NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "blockstore", Config: a.nodeConfig},
	})

	return err
}

// ConfigureSync creates state-sync reactors for each network.
func (a *Archive) ConfigureSync() error {
	return a.initStateSync()
}

// StartStateSync creates separate goroutines for the state-sync process of each network.
func (a *Archive) StartStateSync() error {
	return a.startStateSync()
}

// initStateSync initialize a [statesync.Reactor] instance per each network.
func (a *Archive) initStateSync() error {
	// First make sure the ABCI is setup correctly
	if a.abciClient == nil {
		return fmt.Errorf("missing ABCI client (proxyApp) for archive state-sync")
	}

	// Create one state sync reactor per each replicated chain
	// And also create one block store per each replicated chain
	// Note, we do not save actual block information but commits.
	for _, chainId := range a.GetNetworks() {
		// Prometheus does not allow hyphens in metrics names, it must match
		// following regexp: [a-zA-Z_:][a-zA-Z0-9_:]*
		// see also: https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
		metricsNames := a.nodeConfig.Instrumentation.Namespace + "_" + strings.Replace(chainId, "-", "_", -1)
		ssyncMetricsProvider := statesync.PrometheusMetrics(metricsNames, "chain_id", chainId)

		stateSyncReactor := statesync.NewReactor(
			*a.nodeConfig.StateSync, // Enabled for historical nodes
			a.abciClient.Snapshot(chainId),
			a.abciClient.Query(chainId),
			ssyncMetricsProvider,
		)
		stateSyncReactor.SetLogger(a.logger.With("module", "statesync"))
		a.stateSyncReactors[chainId] = NewChainInstance(chainId, stateSyncReactor)

		blockStore := bs.NewBlockStore(
			a.blockDatabases[chainId],
			bs.WithCompaction(a.nodeConfig.Storage.Compact, a.nodeConfig.Storage.CompactionInterval),
			bs.WithDBKeyLayout(a.nodeConfig.Storage.ExperimentalKeyLayout),
		)

		a.blockStores[chainId] = NewChainInstance(chainId, blockStore)
	}

	return nil
}

// TODO(midas): executeStateSync(): one iteration of state-sync to be done for every block.
// TODO(midas): This process doesn't work until we create the p2p.Switch in archive_node.go.
func (a *Archive) startStateSync() error {
	// Here, we create one goroutine per replicated chain and each starts a
	// CometBFT state-sync process using the corresponding reactor.
	//
	// CAUTION - EXPERIMENTAL:
	// Running the following code is highly unrecommended in
	// a production environment. Please use this feature with
	// caution as it is still being actively developed.
	for _, syncChainId := range a.GetNetworks() {
		if _, ok := a.stateSyncReactors[syncChainId]; !ok {
			return fmt.Errorf("missing state-sync reactor for ChainID: %s", syncChainId)
		}

		if _, ok := a.blockStores[syncChainId]; !ok {
			return fmt.Errorf("missing block store service for ChainID: %s", syncChainId)
		}

		// Type-assertion makes sure we have a [*statesync.Reactor]
		stateSyncReactor := a.stateSyncReactors[syncChainId].GetInstance().(*statesync.Reactor)

		// Retrieve per-network state-sync configuration
		stateSyncConfig, err := a.chainRegistry.GetStateSyncConfig(syncChainId)
		if err != nil {
			return fmt.Errorf("could not start state-sync: %w", err)
		}

		// Type-assertion makes sure we have a [*bs.BlockStore]
		blockStoreService := a.blockStores[syncChainId].GetInstance().(*bs.BlockStore)

		go func(
			chainId string,
			archive *Archive,
			ssR *statesync.Reactor,
			blockStore *bs.BlockStore,
			conf *config.StateSyncConfig,
		) {
			storageConfig := config.DefaultStorageConfig()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			stateProvider, err := statesync.NewLightClientStateProviderWithDBKeyVersion(ctx,
				chainId,
				sm.InitStateVersion, // always start from beginning
				int64(1),            // always start from block 1
				conf.RPCServers,
				light.TrustOptions{
					Period: conf.TrustPeriod,
					Height: conf.TrustHeight,
					Hash:   conf.TrustHashBytes(),
				},
				ssR.Logger.With("module", "light"),
				storageConfig.ExperimentalKeyLayout, // "v1"
			)
			if err != nil {
				// State-sync not possible, stop here!
				cmtos.Exit(fmt.Sprintf(`startStateSync: could not start state-sync light state provider:
				%v\n`, err))
			}

			chainHistoryStore := archive.GetStateStore(chainId).(*ChainHistoryStore)
			go func() {
				ssR.Logger.Info(fmt.Sprintf("Starting new state-sync iteration for ChainID: %s", chainId))

				state, commit, err := ssR.Sync(stateProvider, conf.DiscoveryTime)
				if err != nil {
					ssR.Logger.Error("State sync failed", "err", err)
					return
				}
				err = chainHistoryStore.DBStore.Bootstrap(state)
				if err != nil {
					ssR.Logger.Error("Failed to bootstrap node with new state", "err", err)
					return
				}

				// After bootstrap, we must wrap back to HistoricalState.
				// If the above worked, we may re-wrap to HistoricalState.
				err = chainHistoryStore.Save(state)
				if err != nil {
					ssR.Logger.Error("Failed to store bootstrap state machine", "err", err)
					return
				}

				err = blockStore.SaveSeenCommit(state.LastBlockHeight, commit)
				if err != nil {
					ssR.Logger.Error("Failed to store last seen commit", "err", err)
					return
				}

				ssR.Logger.Info(fmt.Sprintf("Executed state-sync iteration for ChainID: %s", chainId))
			}()

		}(syncChainId, a, stateSyncReactor, blockStoreService, stateSyncConfig)
	}

	return nil
}
