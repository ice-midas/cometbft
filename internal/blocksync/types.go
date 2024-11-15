package blocksync

import (
	cmtbs "github.com/ice-blockchain/cometbft/api/cometbft/blocksync/v1"
	"github.com/ice-blockchain/cometbft/types"
)

var (
	_ types.Wrapper = &cmtbs.StatusRequest{}
	_ types.Wrapper = &cmtbs.StatusResponse{}
	_ types.Wrapper = &cmtbs.NoBlockResponse{}
	_ types.Wrapper = &cmtbs.BlockResponse{}
	_ types.Wrapper = &cmtbs.BlockRequest{}
)
