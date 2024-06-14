package multiplex

import (
	"fmt"

	"github.com/cometbft/cometbft/internal/evidence"
	mempl "github.com/cometbft/cometbft/mempool"
)

type MultiplexServiceRegistry struct {
	WaitStateSync MultiplexFlag
	WaitBlockSync MultiplexFlag

	// stores all multiplex resources in memory
	// TODO: benchmark and verify memory footprint
	AddressBooks    MultiplexAddressBook
	ChainStates     MultiplexState
	ConsensusStates MultiplexConsensus
	StateStores     MultiplexStateStore
	BlockStores     MultiplexBlockStore
	IndexerServices MultiplexIndexerService
	AppConns        MultiplexAppConn
	EventBuses      MultiplexEventBus
	EventSwitches   MultiplexSwitch
	Pruners         MultiplexPruner
}

func (mx *MultiplexServiceRegistry) getServices(userScopeHash string) (*NodeServices, error) {
	if _, ok := mx.EventSwitches[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find event switch with scope hash %s", userScopeHash)
	}

	if _, ok := mx.ChainStates[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find state with scope hash %s", userScopeHash)
	}

	if _, ok := mx.StateStores[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find state store with scope hash %s", userScopeHash)
	}

	if _, ok := mx.BlockStores[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find state store with scope hash %s", userScopeHash)
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

	sw := mx.EventSwitches[userScopeHash]
	addrBook := mx.AddressBooks[userScopeHash]
	indexerService := mx.IndexerServices[userScopeHash]
	state := mx.ChainStates[userScopeHash]
	consensusState := mx.ConsensusStates[userScopeHash]
	proxyApp := mx.AppConns[userScopeHash]
	eventBus := mx.EventBuses[userScopeHash]
	pruner := mx.Pruners[userScopeHash]
	stateStore := mx.StateStores[userScopeHash]
	blockStore := mx.BlockStores[userScopeHash]
	stateSync := mx.WaitStateSync[userScopeHash]
	blockSync := mx.WaitBlockSync[userScopeHash]

	// Mempool and evidence pool read from reactors
	mempool := (sw.Reactor("MEMPOOL").(*mempl.Reactor)).GetMempoolPtr()
	evpool := (sw.Reactor("EVIDENCE").(*evidence.Reactor)).GetPoolPtr()

	return &NodeServices{
		addrBook:       addrBook,
		sw:             sw.Switch,
		eventBus:       eventBus,
		indexerService: indexerService,
		state:          &state.State,
		consensusState: consensusState,
		proxyApp:       proxyApp,
		mempool:        mempool,
		evidencePool:   evpool,
		stateStore:     stateStore,
		blockStore:     blockStore.BlockStore,
		pruner:         pruner,
		stateSync:      stateSync,
		blockSync:      blockSync,
	}, nil
}
