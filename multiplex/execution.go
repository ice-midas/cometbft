package multiplex

import (
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
)

// type UserScopedBlockExecutor struct {
// 	UserAddress string
// 	Scope       string
// 	ScopeHash   string
// 	*sm.BlockExecutor
// }

// NewMultiplexBlockExecutor returns a new BlockExecutor with a NopEventBus.
// Call SetEventBus to provide one.
func NewMultiplexBlockExecutor(
	stateStore *UserScopedStateStore,
	logger log.Logger,
	proxyApp proxy.AppConnConsensus,
	mempool mempool.Mempool,
	evpool sm.EvidencePool,
	blockStore *UserScopedBlockStore,
	options ...sm.BlockExecutorOption,
) *sm.BlockExecutor {
	res := sm.NewBlockExecutor(
		stateStore,
		logger,
		proxyApp,
		mempool,
		evpool,
		blockStore,
		options...,
	)

	// res := &UserScopedBlockExecutor{
	// 	UserAddress:   stateStore.UserAddress,
	// 	Scope:         stateStore.Scope,
	// 	ScopeHash:     stateStore.ScopeHash,
	// 	BlockExecutor: be,
	// }

	for _, option := range options {
		option(res)
	}

	return res
}
