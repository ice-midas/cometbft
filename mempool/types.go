package mempool

import (
	memprotos "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	"github.com/ice-blockchain/cometbft/types"
)

var (
	_ types.Wrapper   = &memprotos.Txs{}
	_ types.Unwrapper = &memprotos.Message{}
)
