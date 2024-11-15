package reactor

import (
	"github.com/ice-blockchain/cometbft/abci/example/kvstore"
	"github.com/ice-blockchain/cometbft/config"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/proxy"
)

var mempool mempl.Mempool

func init() {
	app := kvstore.NewInMemoryApplication()
	cc := proxy.NewLocalClientCreator(app)
	appConnMem, _ := cc.NewABCIMempoolClient()
	err := appConnMem.Start()
	if err != nil {
		panic(err)
	}

	cfg := config.DefaultMempoolConfig()
	cfg.Broadcast = false
	mempool = mempl.NewCListMempool(cfg, appConnMem, 0)
}

func Fuzz(data []byte) int {
	_, err := mempool.CheckTx(data, "")
	if err != nil {
		return 0
	}

	return 1
}
