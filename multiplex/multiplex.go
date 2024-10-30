package multiplex

import (
	"context"

	"github.com/cometbft/cometbft/config"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/internal/evidence"
	"github.com/cometbft/cometbft/libs/service"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/txindex"
	bs "github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

// createMultiplexNodesWithServices creates the underlying [node.Node] instances
// that will be running consensus instances concurrently.
//
// Note that this method must be called after having fully configured all
// the required services for running a nodes multiplex. You should probably
// not have to call this method directly, see [NewNodesMultiplex] instead.
//
// Note also that this method does *not* call the Start() method for the
// created node instances. It is important to note that each node instance's
// Start() method must be called in a separate goroutine to permit concurrent
// consensus instances, blocks production and state machines replication.
func (reactor *Reactor) createMultiplexNodesWithServices(
	ctx context.Context,
	options ...node.Option,
) (
	nodesMultiplex MultiplexMap[*node.Node],
	err error,
) {
	// We shall iterate through all known networks and create separate
	// multiplex transports and event switches for each replicated chain.
	chainRegistry := reactor.GetChainRegistry()

	// Used to retrieve configuration and state per chain.
	genesisDocProvider := reactor.GetGenesisProvider()
	serviceProvider := reactor.GetServicesProvider()
	configProvider := reactor.GetInstanceProvider(KEY_CONFIG)
	statesProvider := reactor.GetInstanceProvider(KEY_STATE)
	privvalProvider := reactor.GetInstanceProvider(KEY_PRIVVAL)
	switchProvider := reactor.GetInstanceProvider(KEY_P2P_SWITCH)
	transportProvider := reactor.GetInstanceProvider(KEY_P2P_TRANSPORT)
	stateStoreProvider := reactor.GetInstanceProvider(KEY_STORE_STATE)
	blockStoreProvider := reactor.GetInstanceProvider(KEY_STORE_BLOCK)
	stateSyncFlagProvider := reactor.GetInstanceProvider(KEY_FLAG_STATESYNC)

	// Retrieve ordered list of networks
	replicatedChains := chainRegistry.GetChains()
	numReplicatedChains := len(replicatedChains)

	// Allocate return objects
	nodesMultiplex = make(MultiplexMap[*node.Node], numReplicatedChains)

	// We iterate through an ordered list of known networks to create
	// one instance of [node.Node] for each replicated chain.
	//
	// This notably permits to keep backwards-compatibility with CometBFT.
	for _, chainId := range replicatedChains {
		// Config
		genesisDoc := genesisDocProvider(chainId)
		cfgOverwrite := configProvider(chainId).(*config.Config)
		privValidator := privvalProvider(chainId).(types.PrivValidator)

		// P2P
		eventSwitch := switchProvider(chainId).(*p2p.Switch)
		p2pTransport := transportProvider(chainId).(*p2p.MultiplexTransport)
		pexAddrBook := eventSwitch.GetAddrBook().(pex.AddrBook)

		// Consensus
		eventBus := serviceProvider(KEY_EVENTBUS, chainId).(*types.EventBus)
		proxyApp := reactor.abciClient.ToAppConns(chainId)
		memplReactor := serviceProvider(KEY_REACTOR_MEMPOOL, chainId).(*mempl.Reactor)
		consensusReactor := serviceProvider(KEY_REACTOR_CONSENSUS, chainId).(*cs.Reactor)
		evidenceReactor := serviceProvider(KEY_REACTOR_EVIDENCE, chainId).(*evidence.Reactor)
		indexerService := serviceProvider(KEY_INDEXERS, chainId).(*txindex.IndexerService)
		pruner := serviceProvider(KEY_PRUNER, chainId).(*sm.Pruner)

		// State/Blocks
		shouldStateSync := stateSyncFlagProvider(chainId).(bool)
		stateMachine := statesProvider(chainId).(sm.State)
		stateStore := stateStoreProvider(chainId).(*ChainStateStore)
		blockStore := blockStoreProvider(chainId).(*bs.BlockStore)

		nodeInstance := node.NewNodeWithServices(
			cfgOverwrite,
			genesisDoc,
			reactor.nodeInfo.GetNodeInfo(chainId),
			reactor.nodeKey,
			privValidator,
			pexAddrBook,
			p2pTransport,
			eventSwitch,
			eventBus,
			proxyApp,
			memplReactor.GetMempoolPtr(),
			evidenceReactor.GetPoolPtr(),
			pruner,
			indexerService,
			stateStore,
			blockStore,
			consensusReactor.GetState(), // cs.State
			shouldStateSync,
			stateMachine, // stateSyncGenesis (sm.State)
		)

		nodeInstance.BaseService = *service.NewBaseService(
			reactor.logger,
			"Node",
			nodeInstance,
		)

		// Apply custom node.Option configuration
		for _, option := range options {
			option(nodeInstance)
		}

		// Prepare registerable instances mapped to ChainID
		nodesMultiplex[chainId] = NewChainInstance(chainId, nodeInstance)
	}

	return nodesMultiplex, nil
}
