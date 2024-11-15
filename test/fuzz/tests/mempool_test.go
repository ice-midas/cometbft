//go:build gofuzz || go1.20

package tests

import (
	"testing"

	abciclient "github.com/ice-blockchain/cometbft/abci/client"
	"github.com/ice-blockchain/cometbft/abci/example/kvstore"
	"github.com/ice-blockchain/cometbft/config"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
	mempl "github.com/ice-blockchain/cometbft/mempool"
)

func FuzzMempool(f *testing.F) {
	app := kvstore.NewInMemoryApplication()
	mtx := new(cmtsync.Mutex)
	conn := abciclient.NewLocalClient(mtx, app)
	err := conn.Start()
	if err != nil {
		panic(err)
	}

	cfg := config.DefaultMempoolConfig()
	cfg.Broadcast = false

	mp := mempl.NewCListMempool(cfg, conn, 0)

	f.Fuzz(func(_ *testing.T, data []byte) {
		_, _ = mp.CheckTx(data, "")
	})
}
