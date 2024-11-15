package psql

import (
	"github.com/ice-blockchain/cometbft/state/indexer"
	"github.com/ice-blockchain/cometbft/state/txindex"
)

var (
	_ indexer.BlockIndexer = BackportBlockIndexer{}
	_ txindex.TxIndexer    = BackportTxIndexer{}
)
