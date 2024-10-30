package multiplex

import (
	"fmt"

	sm "github.com/cometbft/cometbft/state"
	bs "github.com/cometbft/cometbft/store"
)

// InitMultiplexStates loads a state multiplex using the reactor's
// instance provider to retrieve a state database instance by ChainID.
//
// First, a state database instance is retrieved, then the GenesisDoc checksum
// is validated against the hash stored in the state database.
// Next, a snapshottable [ChainStateStore] instance is created around the
// state database instance for the respective replicated chain.
//
// And finally, this method will *load the state machine* using either the
// database or the GenesisDoc.
//
// This method also registers instances in the multiplexRegistry:
// - `state`: the [sm.State] state machine instances.
// - `stateStore`: the [sm.Store] instance attached to the database.
func (reactor *Reactor) InitMultiplexStates() error {
	// Used for database key layouts
	globalConfig := reactor.GetNodeConfig()

	// Used for retrieving GenesisDoc instance by chain
	genesisDocProvider := reactor.GetGenesisProvider()

	// Used for retrieving state database instance by chain
	stateMachineProvider := reactor.GetInstanceProvider(KEY_DB_SM)

	// Validate genesis configuration
	// Then get genesis doc set hashes from dbs or update
	for _, chainId := range reactor.GetNetworks() {
		// Validate per-chain genesis doc
		genesisDoc := genesisDocProvider(chainId)
		if err := genesisDoc.ValidateAndComplete(); err != nil {
			return fmt.Errorf("error in genesis doc for ChainID %s: %w", chainId, err)
		}

		// Retrieve this chain's state database instance
		stateDB := stateMachineProvider(chainId).(*ChainDB)

		// Validate the genesis doc hash vs. database
		if err := ValidateGenesisDocChecksum(stateDB, genesisDoc); err != nil {
			return err
		}

		// Initialize a ChainStateStore (snapshottable)
		dbKeyLayoutVersion := globalConfig.Storage.ExperimentalKeyLayout
		stateStore := &ChainStateStore{
			ChainID: chainId,
			DBStore: sm.NewDBStore(stateDB, sm.StoreOptions{
				DiscardABCIResponses: false,
				DBKeyLayout:          dbKeyLayoutVersion,
			}).(*sm.DBStore),
		}

		// Load the state from database or GenesisDoc
		chainState, err := stateStore.LoadFromDBOrGenesisDoc(genesisDoc)
		if err != nil {
			return err
		}

		// Prepare registerable instance mapped to ChainID
		reactor.RegisterInstance(KEY_STATE, chainId, chainState)
		reactor.RegisterInstance(KEY_STORE_STATE, chainId, stateStore)
	}

	return nil
}

// InitMultiplexBlockStores loads a [bs.BlockStore] multiplex using
// the reactor's instance provider to retrieve a blockstore database instance
// by ChainID.
//
// First, a blockstore database instance is retrieved, then a non-snapshottable
// [bs.BlockStore] instance is created around the blockstore database instance
// for the respective replicated chain.
//
// This method also registers instances in the multiplexRegistry:
// - `blockStore`: the [bs.BlockStore] instance attached to the database.
func (reactor *Reactor) InitMultiplexBlockStores() error {
	// Used for database key layouts
	globalConfig := reactor.GetNodeConfig()

	// Used for retrieving blockstore database instance by chain
	blockStoreProvider := reactor.GetInstanceProvider(KEY_DB_BS)

	for _, chainId := range reactor.GetNetworks() {
		// Retrieve this chain's state database instance
		blockstoreDB := blockStoreProvider(chainId).(*ChainDB)

		// Initialize a [bs.BlockStore] (not snapshottable)
		blockStore := bs.NewBlockStore(
			blockstoreDB,
			bs.WithCompaction(globalConfig.Storage.Compact, globalConfig.Storage.CompactionInterval),
			bs.WithDBKeyLayout(globalConfig.Storage.ExperimentalKeyLayout),
		)

		// Prepare registerable instance mapped to ChainID
		reactor.RegisterInstance(KEY_STORE_BLOCK, chainId, blockStore)
	}

	return nil
}
