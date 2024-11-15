package evidence

import (
	"github.com/ice-blockchain/cometbft/types"
)

//go:generate ../../scripts/mockery_generate.sh BlockStore

type BlockStore interface {
	LoadBlockMeta(height int64) *types.BlockMeta
	LoadBlockCommit(height int64) *types.Commit
	Height() int64
}
