package multiplex

import (
	"context"
	"errors"
	"fmt"
	"slices"

	cfg "github.com/cometbft/cometbft/config"
	bc "github.com/cometbft/cometbft/internal/blocksync"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/internal/evidence"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/service"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/indexer"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/statesync"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
)

// ReplicatedNodeServices stores pointers to resources that are bootstrapped
// for **one replicated node**, i.e. the address book shall hold only entries
// relevant to P2P communications of only one of the replicated blockchains.
type ReplicatedNodeServices struct {
	addrBook pex.AddrBook

	// Handlers
	sw           *p2p.Switch
	eventBus     *types.EventBus
	proxyApp     *proxy.AppConns
	mempool      *mempl.Mempool
	evidencePool *evidence.Pool
	pruner       *sm.Pruner

	// State
	indexerService *txindex.IndexerService
	consensusState *cs.State
	state          *sm.State
	stateStore     sm.Store
	blockStore     *store.BlockStore

	// Flags
	stateSync bool
	blockSync bool
}

// XXX naming convention: should be ScopedNode
type ReplicatedChainNode struct {
	ScopeHash string
	*node.Node
}

type NodeMultiplex struct {
	config        *cfg.Config
	privValidator *types.PrivValidator
	nodeInfo      p2p.NodeInfo
	nodeKey       *p2p.NodeKey // our node privkey

	// TBI: keep Nodes or split functionality
	Nodes map[string]*ReplicatedChainNode

	WaitStateSync map[string]bool
	WaitBlockSync map[string]bool

	// XXX: every Node instance stores their own resources in memory
	// such that the multiplex storage here is heavily redundant.
	ChainStates     MultiplexState
	StateStores     MultiplexStateStore
	BlockStores     MultiplexBlockStore
	AddressBooks    ScopedAddressBook
	EventSwitches   SwitchMultiplex
	EventBuses      map[string]*types.EventBus
	ConsensusStates map[string]*cs.State
	IndexerServices map[string]*txindex.IndexerService
	AppConns        map[string]*proxy.AppConns
	Mempools        map[string]*mempl.Mempool
	EvidencePools   map[string]*evidence.Pool
	Pruners         map[string]*sm.Pruner
}

// NewMultiplexNode returns a new, ready to go, multiplex CometBFT Node.
// Multiplex-mode refers to the user-scoped replication strategy.
func NewMultiplexNode(ctx context.Context,
	config *cfg.Config,
	privValidator types.PrivValidator,
	nodeKey *p2p.NodeKey,
	clientCreator proxy.ClientCreator,
	genesisDocProvider node.GenesisDocProvider,
	dbProvider cfg.DBProvider,
	metricsProvider node.MetricsProvider,
	logger log.Logger,
	options ...node.Option,
) (*NodeMultiplex, error) {
	if config.BaseConfig.DBBackend == "boltdb" || config.BaseConfig.DBBackend == "cleveldb" {
		logger.Info("WARNING: BoltDB and GoLevelDB are deprecated and will be removed in a future release. Please switch to a different backend.")
	}

	// Augment user configuration to multiplex capacity
	mxUserConfig := NewUserConfig(config.Replication, config.UserScopes)

	blockStoreMultiplexDB,
		stateMultiplexDB,
		indexerMultiplexDB,
		evidenceMultiplexDB,
		err := initDBs(config, dbProvider)
	if err != nil {
		return nil, err
	}

	multiplexState, icsGenDoc, err := LoadMultiplexStateFromDBOrGenesisDocProviderWithConfig(stateMultiplexDB, genesisDocProvider, config.Storage.GenesisHash, config)
	if err != nil {
		return nil, err
	}

	// Global metrics are necessary to initialize stores and p2pMetrics is
	// registered globally because P2P implementation is not replicated.
	globalMetricsProvider := GlobalMetricsProvider(config.Instrumentation)
	smMetrics, bstMetrics, p2pMetrics := globalMetricsProvider("__global__")

	multiplexStateStore := NewMultiplexStateStore(stateMultiplexDB, sm.StoreOptions{
		DiscardABCIResponses: config.Storage.DiscardABCIResponses,
		Metrics:              smMetrics,
		Compact:              config.Storage.Compact,
		CompactionInterval:   config.Storage.CompactionInterval,
		Logger:               logger,
		DBKeyLayout:          config.Storage.ExperimentalKeyLayout,
	})

	multiplexBlockStore := NewMultiplexBlockStore(
		blockStoreMultiplexDB,
		store.WithMetrics(bstMetrics),
		store.WithCompaction(config.Storage.Compact, config.Storage.CompactionInterval),
		store.WithDBKeyLayout(config.Storage.ExperimentalKeyLayout),
		store.WithDBKeyLayout(config.Storage.ExperimentalKeyLayout),
	)

	// The key will be deleted if it existed.
	// Not checking whether the key is there in case the genesis file was larger than
	// the max size of a value (in rocksDB for example), which would cause the check
	// to fail and prevent the node from booting.
	logger.Info("WARNING: deleting genesis file from database if present, the database stores a hash of the original genesis file now")

	// For each of the replicated (user-scoped) chains, we store pointers to services
	replicatedChainsScopeHashes := mxUserConfig.ScopeHashes()
	numReplicatedChains := len(replicatedChainsScopeHashes)
	multiplexEventBus := make(map[string]*types.EventBus, numReplicatedChains)
	multiplexPruner := make(map[string]*sm.Pruner, numReplicatedChains)
	multiplexTxIndexer := make(map[string]*txindex.TxIndexer, numReplicatedChains)
	multiplexIndexer := make(map[string]*txindex.IndexerService, numReplicatedChains)
	multiplexBlockIndexer := make(map[string]*indexer.BlockIndexer, numReplicatedChains)
	multiplexAppConns := make(map[string]*proxy.AppConns, numReplicatedChains)
	multiplexMempoolReactor := make(map[string]*mempl.Reactor, numReplicatedChains)
	multiplexMempool := make(map[string]*mempl.Mempool, numReplicatedChains)
	//multiplexPexReactor := make(map[string]*pex.Reactor, numReplicatedChains)
	multiplexBlockSyncReactor := make(map[string]*p2p.Reactor, numReplicatedChains)
	multiplexStateSyncReactor := make(map[string]*statesync.Reactor, numReplicatedChains)
	multiplexConsensusReactor := make(map[string]*cs.Reactor, numReplicatedChains)
	multiplexConsensusState := make(map[string]*cs.State, numReplicatedChains)
	multiplexEvidenceReactor := make(map[string]*evidence.Reactor, numReplicatedChains)
	multiplexEvidencePool := make(map[string]*evidence.Pool, numReplicatedChains)
	stateSyncEnabledByScope := make(map[string]bool, numReplicatedChains)
	blockSyncEnabledByScope := make(map[string]bool, numReplicatedChains)

	// For each scope hash, we replicate state and block store, and create
	// event bus, indexers and reactors.
	for _, userScopeHash := range replicatedChainsScopeHashes {
		// ------------------------------------------------------------------------
		// ENVIRONMENT

		multiplexMetricsProvider := MultiplexMetricsProvider(config.Instrumentation)

		// Retrieve genesis doc for current scope hash
		genDoc, err := icsGenDoc.GenesisDocByScope(userScopeHash)
		if err != nil {
			return nil, err
		}

		scopeId := NewScopeIDFromHash(userScopeHash)
		logger.With("subchain", scopeId.Fingerprint())

		// Initialize metrics with user scope hash
		csMetrics,
			memplMetrics,
			smMxMetrics,
			abciMetrics,
			bsMetrics,
			ssMetrics := multiplexMetricsProvider(genDoc.ChainID, userScopeHash)

		// Retrieve state database and state machine
		stateDB, stateMachine, stateStore, blockStore, err := getReplicatedServices(
			&stateMultiplexDB,
			&multiplexState,
			&multiplexStateStore,
			&multiplexBlockStore,
			userScopeHash,
		)
		if err != nil {
			return nil, err
		}

		err = stateDB.Delete(genesisDocKey)
		if err != nil {
			logger.Error("Failed to delete genesis doc from DB ", err)
		}

		logger.Info("Blockstore version", "version", blockStore.GetVersion())

		// ------------------------------------------------------------------------
		// APP / EVENTS / INDEXERS

		// Create the proxyApp and establish connections to the ABCI app (consensus, mempool, query).
		proxyApp, err := createAndStartProxyAppConns(clientCreator, logger, abciMetrics)
		if err != nil {
			return nil, err
		}

		// EventBus and IndexerService must be started before the handshake because
		// we might need to index the txs of the replayed block as this might not have happened
		// when the node stopped last time (i.e. the node stopped after it saved the block
		// but before it indexed the txs)
		eventBus, err := createAndStartEventBus(logger)
		if err != nil {
			return nil, err
		}

		indexerService, txIndexer, blockIndexer, err := createAndStartIndexerService(config,
			genDoc.ChainID, indexerMultiplexDB, eventBus, logger, userScopeHash)
		if err != nil {
			return nil, err
		}

		// ------------------------------------------------------------------------
		// PRIV VALIDATOR

		// If an address is provided, listen on the socket for a connection from an
		// external signing process.
		if config.PrivValidatorListenAddr != "" {
			// FIXME: we should start services inside OnStart
			privValidator, err = createAndStartPrivValidatorSocketClient(config.PrivValidatorListenAddr, genDoc.ChainID, logger)
			if err != nil {
				return nil, fmt.Errorf("error with private validator socket client: %w", err)
			}
		}

		pubKey, err := privValidator.GetPubKey()
		if err != nil {
			return nil, fmt.Errorf("can't get pubkey: %w", err)
		}

		// ------------------------------------------------------------------------
		// CONSENSUS

		// Determine whether we should attempt state sync.
		stateSync := config.StateSync.Enable && !onlyValidatorIsUs(*stateMachine, pubKey)
		if stateSync && stateMachine.LastBlockHeight > 0 {
			logger.Info("Found local state with non-zero height, skipping state sync")
			stateSync = false
		}

		// Create the handshaker, which calls RequestInfo, sets the AppVersion on the state,
		// and replays any blocks as necessary to sync CometBFT with the app.
		consensusLogger := logger.With("module", "consensus")
		if !stateSync {
			if err := doHandshake(ctx, *stateStore, *stateMachine, blockStore, genDoc, eventBus, proxyApp, consensusLogger); err != nil {
				return nil, err
			}

			// Reload the state. It will have the Version.Consensus.App set by the
			// Handshake, and may have other modifications as well (ie. depending on
			// what happened during block replay).

			reloadedState, err := stateStore.Load()
			stateMachine = &ScopedState{
				ScopeHash: stateMachine.ScopeHash,
				State:     reloadedState,
			}
			if err != nil {
				return nil, sm.ErrCannotLoadState{Err: err}
			}
		}

		// Determine whether we should do block sync. This must happen after the handshake, since the
		// app may modify the validator set, specifying ourself as the only validator.
		blockSync := !onlyValidatorIsUs(*stateMachine, pubKey)
		waitSync := stateSync || blockSync

		logNodeStartupInfo(*stateMachine, pubKey, logger, consensusLogger)

		mempool, mempoolReactor := createMempoolAndMempoolReactor(config, proxyApp, *stateMachine, waitSync, memplMetrics, logger)

		evidenceReactor, evidencePool, err := createEvidenceReactor(
			config,
			evidenceMultiplexDB,
			stateStore,
			blockStore,
			logger,
			userScopeHash,
		)
		if err != nil {
			return nil, err
		}

		pruner, err := createPruner(
			config,
			txIndexer,
			blockIndexer,
			stateStore,
			blockStore,
			smMxMetrics,
			logger.With("module", "state"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create pruner: %w", err)
		}

		// make block executor for consensus and blocksync reactors to execute blocks
		blockExec := sm.NewBlockExecutor(
			stateStore,
			logger.With("module", "state"),
			proxyApp.Consensus(),
			mempool,
			evidencePool,
			blockStore,
			sm.BlockExecutorWithPruner(pruner),
			sm.BlockExecutorWithMetrics(smMxMetrics),
		)

		offlineStateSyncHeight := int64(0)
		if blockStore.Height() == 0 {
			offlineStateSyncHeight, err = blockExec.Store().GetOfflineStateSyncHeight()
			if err != nil && err.Error() != "value empty" {
				panic(fmt.Sprintf("failed to retrieve statesynced height from store %s with scope hash %s; expected state store height to be %v", err, userScopeHash, stateMachine.LastBlockHeight))
			}
		}
		// Don't start block sync if we're doing a state sync first.
		bcReactor, err := createBlocksyncReactor(config, stateMachine, blockExec, blockStore, blockSync && !stateSync, logger, bsMetrics, offlineStateSyncHeight)
		if err != nil {
			return nil, fmt.Errorf("could not create blocksync reactor: %w", err)
		}

		consensusReactor, consensusState := createConsensusReactor(
			config, stateMachine, blockExec, blockStore, mempool, evidencePool,
			privValidator, csMetrics, waitSync, eventBus, consensusLogger, offlineStateSyncHeight,
		)

		err = stateStore.SetOfflineStateSyncHeight(0)
		if err != nil {
			panic(fmt.Sprintf("failed to reset the offline state sync height %s with scope hash %s", err, userScopeHash))
		}
		// Set up state sync reactor, and schedule a sync if requested.
		// FIXME The way we do phased startups (e.g. replay -> block sync -> consensus) is very messy,
		// we should clean this whole thing up. See:
		// https://github.com/tendermint/tendermint/issues/4644
		stateSyncReactor := statesync.NewReactor(
			*config.StateSync,
			proxyApp.Snapshot(),
			proxyApp.Query(),
			ssMetrics,
		)
		stateSyncReactor.SetLogger(logger.With("module", "statesync"))

		// ------------------------------------------------------------------------
		// MULTIPLEX SERVICES READY
		// Multiplex services are now all ready and configured
		// The replicated (user scoped) chain is fully configured

		multiplexPruner[userScopeHash] = pruner
		multiplexEventBus[userScopeHash] = eventBus
		multiplexTxIndexer[userScopeHash] = &txIndexer
		multiplexIndexer[userScopeHash] = indexerService
		multiplexBlockIndexer[userScopeHash] = &blockIndexer
		multiplexAppConns[userScopeHash] = &proxyApp
		multiplexMempoolReactor[userScopeHash] = mempoolReactor.(*mempl.Reactor)
		multiplexMempool[userScopeHash] = &mempool
		multiplexBlockSyncReactor[userScopeHash] = &bcReactor
		multiplexStateSyncReactor[userScopeHash] = stateSyncReactor
		multiplexConsensusReactor[userScopeHash] = consensusReactor
		multiplexConsensusState[userScopeHash] = consensusState
		multiplexEvidenceReactor[userScopeHash] = evidenceReactor
		multiplexEvidencePool[userScopeHash] = evidencePool
		stateSyncEnabledByScope[userScopeHash] = stateSync
		blockSyncEnabledByScope[userScopeHash] = blockSync
	}
	// End of for loop, following code is run *globally*

	// --------------------------------------------------------------------------
	// GLOBAL NODE RESOURCES

	// --------------------------------------------------------------------------
	// P2P / TRANSPORT / SWITCH

	nodeInfo, err := makeNodeInfo(config, nodeKey, genesisDocProvider, multiplexState)
	if err != nil {
		return nil, err
	}

	transport, peerFilters := createTransport(config, nodeInfo, nodeKey, multiplexAppConns)

	p2pLogger := logger.With("module", "p2p")
	switchMultiplex, err := createSwitches(
		config,
		transport,
		p2pMetrics,
		peerFilters,
		replicatedChainsScopeHashes,
		multiplexMempoolReactor,
		multiplexBlockSyncReactor,
		multiplexStateSyncReactor,
		multiplexConsensusReactor,
		multiplexEvidenceReactor,
		nodeInfo,
		nodeKey,
		p2pLogger,
	)

	if err != nil {
		return nil, fmt.Errorf("could not create event switches: %w", err)
	}

	// Note: AddPersistentPeers() and AddUnconditionalPeers are called
	// in createSwitches() as a first draft to permit adding peers for
	// several different replicated chains / networks.
	//
	// TODO:
	// In a replicated chains scenario, the PersistentPeers and UnconditionalPeers
	// configuration must be replicated on a per-scope basis because the list of
	// validators is different from one replicated chain to the other.

	addressBooks, err := createAddressBooksAndSetOnSwitches(
		config,
		replicatedChainsScopeHashes,
		switchMultiplex,
		p2pLogger,
		nodeKey,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create address books: %w", err)
	}

	// Optionally, start the pex reactors
	//
	// see TODO in node/node.go
	//
	// FIXME: in multiplex, this setting should be per-scope and creating
	// the PEX reactor can be moved to replicated services creation loop
	if config.P2P.PexReactor {
		_, err = createPEXReactorsAndAddToSwitches(
			replicatedChainsScopeHashes,
			addressBooks,
			config,
			switchMultiplex,
			logger,
		)

		if err != nil {
			return nil, fmt.Errorf("could not PEX reactors: %w", err)
		}
	}

	for _, addressBook := range addressBooks {
		// Add private IDs to addrbook to block those peers being added
		addressBook.AddPrivateIDs(splitAndTrimEmpty(config.P2P.PrivatePeerIDs, ",", " "))
	}

	nodeMultiplex := &NodeMultiplex{
		config:        config,
		privValidator: &privValidator,
		nodeInfo:      nodeInfo,
		nodeKey:       nodeKey,

		// TODO: every Node instance stores their own resources in memory
		// such that the multiplex storage in this struct is heavily redundant.
		Nodes: make(map[string]*ReplicatedChainNode, len(replicatedChainsScopeHashes)),

		WaitStateSync: stateSyncEnabledByScope,
		WaitBlockSync: blockSyncEnabledByScope,

		// stores all multiplex resources in memory
		// TODO: benchmark and verify memory footprint
		ChainStates:     multiplexState,
		ConsensusStates: multiplexConsensusState,
		IndexerServices: multiplexIndexer,
		AppConns:        multiplexAppConns,
		Mempools:        multiplexMempool,
		EvidencePools:   multiplexEvidencePool,
		EventBuses:      multiplexEventBus,
		EventSwitches:   switchMultiplex,
	}

	for _, userScopeHash := range replicatedChainsScopeHashes {
		// Error can be skipped because it would have exited before
		genDoc, _ := icsGenDoc.GenesisDocByScope(userScopeHash)

		nodeServices, err := nodeMultiplex.getServices(userScopeHash)
		if err != nil {
			return nil, fmt.Errorf("could not retrieve multiplex services with scope hash %s: %w", userScopeHash, err)
		}

		node := node.NewNodeWithServices(
			config,
			genDoc,
			privValidator,
			nodeInfo,
			nodeKey,
			transport,
			nodeServices.addrBook,

			nodeServices.sw,
			nodeServices.eventBus,
			*nodeServices.proxyApp,
			*nodeServices.mempool,
			nodeServices.evidencePool,
			nodeServices.pruner,

			nodeServices.indexerService,
			nodeServices.stateStore,
			nodeServices.blockStore,
			nodeServices.consensusState,
			nodeServices.stateSync,
			*nodeServices.state,
		)

		node.BaseService = *service.NewBaseService(logger, "Node", node)

		for _, option := range options {
			option(node)
		}

		// stores this node instance in the multiplex
		nodeMultiplex.Nodes[userScopeHash] = &ReplicatedChainNode{
			ScopeHash: userScopeHash,
			Node:      node,
		}
	}

	return nodeMultiplex, nil
}

func (mx *NodeMultiplex) getServices(userScopeHash string) (*ReplicatedNodeServices, error) {

	switchPos := slices.IndexFunc(mx.EventSwitches, func(sw *ScopedSwitch) bool {
		return sw.ScopeHash == userScopeHash
	})

	statePos := slices.IndexFunc(mx.ChainStates, func(s ScopedState) bool {
		return s.ScopeHash == userScopeHash
	})

	stateStorePos := slices.IndexFunc(mx.StateStores, func(ss ScopedStateStore) bool {
		return ss.ScopeHash == userScopeHash
	})

	blockStorePos := slices.IndexFunc(mx.BlockStores, func(bs *ScopedBlockStore) bool {
		return bs.ScopeHash == userScopeHash
	})

	if switchPos < 0 {
		return nil, fmt.Errorf("could not find event switch with scope hash %s", userScopeHash)
	}

	if statePos < 0 {
		return nil, fmt.Errorf("could not find state with scope hash %s", userScopeHash)
	}

	if stateStorePos < 0 {
		return nil, fmt.Errorf("could not find state store with scope hash %s", userScopeHash)
	}

	if blockStorePos < 0 {
		return nil, fmt.Errorf("could not find block store with scope hash %s", userScopeHash)
	}

	if _, ok := mx.AddressBooks[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find address book with scope hash %s", userScopeHash)
	}

	if _, ok := mx.IndexerServices[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find indexer services with scope hash %s", userScopeHash)
	}

	if _, ok := mx.ConsensusStates[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find consensus state with scope hash %s", userScopeHash)
	}

	if _, ok := mx.AppConns[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find proxy app conns with scope hash %s", userScopeHash)
	}

	if _, ok := mx.Mempools[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find mempool with scope hash %s", userScopeHash)
	}

	if _, ok := mx.EvidencePools[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find evidence pool with scope hash %s", userScopeHash)
	}

	if _, ok := mx.EventBuses[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find event bus with scope hash %s", userScopeHash)
	}

	if _, ok := mx.Pruners[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find pruner with scope hash %s", userScopeHash)
	}

	if _, ok := mx.WaitStateSync[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find statesync status with scope hash %s", userScopeHash)
	}

	if _, ok := mx.WaitBlockSync[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find blocksync status with scope hash %s", userScopeHash)
	}

	sw := mx.EventSwitches[switchPos]
	addrBook := mx.AddressBooks[userScopeHash]
	indexerService := mx.IndexerServices[userScopeHash]
	consensusState := mx.ConsensusStates[userScopeHash]
	proxyApp := mx.AppConns[userScopeHash]
	mempool := mx.Mempools[userScopeHash]
	evidencePool := mx.EvidencePools[userScopeHash]
	eventBus := mx.EventBuses[userScopeHash]
	pruner := mx.Pruners[userScopeHash]
	stateStore := mx.StateStores[stateStorePos]
	blockStore := mx.BlockStores[blockStorePos]
	stateSync := mx.WaitStateSync[userScopeHash]
	blockSync := mx.WaitBlockSync[userScopeHash]

	return &ReplicatedNodeServices{
		addrBook:       addrBook,
		sw:             sw.Switch,
		eventBus:       eventBus,
		indexerService: indexerService,
		consensusState: consensusState,
		proxyApp:       proxyApp,
		mempool:        mempool,
		evidencePool:   evidencePool,
		stateStore:     stateStore,
		blockStore:     blockStore.BlockStore,
		pruner:         pruner,
		stateSync:      stateSync,
		blockSync:      blockSync,
	}, nil
}

// ----------------------------------------------------------------------------
//

func makeNodeInfo(
	config *cfg.Config,
	nodeKey *p2p.NodeKey,
	genesisDocProvider node.GenesisDocProvider,
	multiplexState MultiplexState,
) (MultiplexNodeInfo, error) {

	numReplicatedChains := len(multiplexState)
	protocolVersions := make([]MultiplexProtocolVersion, numReplicatedChains)
	supportedNetworks := make([]MultiplexNetwork, numReplicatedChains)

	var icsGenDoc node.IChecksummedGenesisDoc
	icsGenDoc, err := genesisDocProvider()
	if err != nil {
		return MultiplexNodeInfo{}, err
	}

	// Fill ProtocolVersions and Networks fields
	for i, userScopedState := range multiplexState {
		genDoc, _ := icsGenDoc.GenesisDocByScope(userScopedState.ScopeHash)

		protocolVersions[i] = MultiplexProtocolVersion{
			ScopeHash: userScopedState.ScopeHash,
			P2P:       version.P2PProtocol, // global
			Block:     userScopedState.Version.Consensus.Block,
			App:       userScopedState.Version.Consensus.App,
		}

		supportedNetworks[i] = MultiplexNetwork{
			ScopeHash: userScopedState.ScopeHash,
			ChainID:   genDoc.ChainID,
		}
	}

	// FIXME: txIndexer can be disabled but multiplex *always* enables it for now
	txIndexerStatus := "on"
	// if _, ok := txIndexer.(*null.TxIndex); ok {
	// 	txIndexerStatus = "off"
	// }

	nodeInfo := MultiplexNodeInfo{
		ProtocolVersions: protocolVersions,
		DefaultNodeID:    nodeKey.ID(),
		Networks:         supportedNetworks,
		Version:          version.CMTSemVer,
		Channels: []byte{
			bc.BlocksyncChannel,
			cs.StateChannel, cs.DataChannel, cs.VoteChannel, cs.VoteSetBitsChannel,
			mempl.MempoolChannel,
			evidence.EvidenceChannel,
			statesync.SnapshotChannel, statesync.ChunkChannel,
		},
		Moniker: config.Moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    txIndexerStatus,
			RPCAddress: config.RPC.ListenAddress,
		},
	}

	if config.P2P.PexReactor {
		nodeInfo.Channels = append(nodeInfo.Channels, pex.PexChannel)
	}

	lAddr := config.P2P.ExternalAddress

	if lAddr == "" {
		lAddr = config.P2P.ListenAddress
	}

	nodeInfo.ListenAddr = lAddr

	err = nodeInfo.Validate()
	return nodeInfo, err
}

func createPruner(
	config *cfg.Config,
	txIndexer txindex.TxIndexer,
	blockIndexer indexer.BlockIndexer,
	stateStore *ScopedStateStore,
	blockStore *ScopedBlockStore,
	metrics *sm.Metrics,
	logger log.Logger,
) (*sm.Pruner, error) {
	if err := initApplicationRetainHeight(stateStore); err != nil {
		return nil, err
	}

	prunerOpts := []sm.PrunerOption{
		sm.WithPrunerInterval(config.Storage.Pruning.Interval),
		sm.WithPrunerMetrics(metrics),
	}

	if config.Storage.Pruning.DataCompanion.Enabled {
		err := initCompanionRetainHeights(
			stateStore,
			config.Storage.Pruning.DataCompanion.InitialBlockRetainHeight,
			config.Storage.Pruning.DataCompanion.InitialBlockResultsRetainHeight,
		)
		if err != nil {
			return nil, err
		}
		prunerOpts = append(prunerOpts, sm.WithPrunerCompanionEnabled())
	}

	return sm.NewPruner(stateStore, blockStore, blockIndexer, txIndexer, logger, prunerOpts...), nil
}

// Set the initial application retain height to 0 to avoid the data companion
// pruning blocks before the application indicates it is OK. We set this to 0
// only if the retain height was not set before by the application.
func initApplicationRetainHeight(stateStore *ScopedStateStore) error {
	if _, err := stateStore.GetApplicationRetainHeight(); err != nil {
		if errors.Is(err, sm.ErrKeyNotFound) {
			return stateStore.SaveApplicationRetainHeight(0)
		}
		return err
	}
	return nil
}

// Sets the data companion retain heights if one of two possible conditions is
// met:
// 1. One or more of the retain heights has not yet been set.
// 2. One or more of the retain heights is currently 0.
func initCompanionRetainHeights(stateStore *ScopedStateStore, initBlockRH, initBlockResultsRH int64) error {
	curBlockRH, err := stateStore.GetCompanionBlockRetainHeight()
	if err != nil && !errors.Is(err, sm.ErrKeyNotFound) {
		return fmt.Errorf("failed to obtain companion block retain height: %w", err)
	}
	if curBlockRH == 0 {
		if err := stateStore.SaveCompanionBlockRetainHeight(initBlockRH); err != nil {
			return fmt.Errorf("failed to set initial data companion block retain height: %w", err)
		}
	}
	curBlockResultsRH, err := stateStore.GetABCIResRetainHeight()
	if err != nil && !errors.Is(err, sm.ErrKeyNotFound) {
		return fmt.Errorf("failed to obtain companion block results retain height: %w", err)
	}
	if curBlockResultsRH == 0 {
		if err := stateStore.SaveABCIResRetainHeight(initBlockResultsRH); err != nil {
			return fmt.Errorf("failed to set initial data companion block results retain height: %w", err)
		}
	}
	return nil
}
