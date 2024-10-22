package proxy

import (
	"fmt"

	abcicli "github.com/cometbft/cometbft/abci/client"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/service"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
)

// -----------------------------------------------------------------------------------------
// multiplexAppConn

// sharedConnClients contains shared ABCI client instances.
type sharedConnClients struct {
	mtx *cmtsync.Mutex

	consensus abcicli.Client
	mempool   abcicli.Client
	query     abcicli.Client
	snapshot  abcicli.Client
}

// multiplexAppConn implements AppConns.
type multiplexAppConn struct {
	service.BaseService

	chainIds []string

	metrics *Metrics

	connsMutex     *cmtsync.RWMutex
	consensusConns map[string]AppConnConsensus
	mempoolConns   map[string]AppConnMempool
	queryConns     map[string]AppConnQuery
	snapshotConns  map[string]AppConnSnapshot

	sharedClients *sharedConnClients

	clientCreator ClientCreator
}

var _ ChainConns = (*multiplexAppConn)(nil)

// NewMultiplexAppConn makes all necessary abci connections to the application
// for a slice of replicated chains by ChainID.
func NewMultiplexAppConn(
	chainIds []string,
	clientCreator ClientCreator,
	metrics *Metrics,
) ChainConns {
	mac := &multiplexAppConn{
		chainIds:   chainIds,
		metrics:    metrics,
		connsMutex: new(cmtsync.RWMutex),

		clientCreator: clientCreator,
		sharedClients: &sharedConnClients{
			mtx: new(cmtsync.Mutex),
		},
	}
	mac.BaseService = *service.NewBaseService(nil, "multiplexAppConn", mac)
	return mac
}

// Mempool implements [ChainConns]
func (conn *multiplexAppConn) Mempool(chainId string) AppConnMempool {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.mempoolConns[chainId]
}

// Consensus implements [ChainConns]
func (conn *multiplexAppConn) Consensus(chainId string) AppConnConsensus {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.consensusConns[chainId]
}

// Query implements [ChainConns]
func (conn *multiplexAppConn) Query(chainId string) AppConnQuery {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.queryConns[chainId]
}

// Snapshot implements [ChainConns]
func (conn *multiplexAppConn) Snapshot(chainId string) AppConnSnapshot {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.snapshotConns[chainId]
}

// ToAppConns converts the instance to be AppConns compatible
// Note that this method uses multiAppConn, not multiplexAppConn.
//
// ToAppConns implements [ChainConns]
func (conn *multiplexAppConn) ToAppConns(chainId string) AppConns {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()

	// Note: this instance uses the legacy AppConns implementation
	return &multiAppConn{
		metrics:       conn.metrics,
		consensusConn: conn.consensusConns[chainId],
		mempoolConn:   conn.mempoolConns[chainId],
		queryConn:     conn.queryConns[chainId],
		snapshotConn:  conn.snapshotConns[chainId],

		consensusConnClient: conn.sharedClients.consensus,
		mempoolConnClient:   conn.sharedClients.mempool,
		queryConnClient:     conn.sharedClients.query,
		snapshotConnClient:  conn.sharedClients.snapshot,

		clientCreator: conn.clientCreator,
	}
}

// OnStart implements [service.Service]
func (conn *multiplexAppConn) OnStart() error {
	if err := conn.startQueryClient(); err != nil {
		return err
	}
	if err := conn.startSnapshotClient(); err != nil {
		conn.stopAllClients()
		return err
	}
	if err := conn.startMempoolClient(); err != nil {
		conn.stopAllClients()
		return err
	}
	if err := conn.startConsensusClient(); err != nil {
		conn.stopAllClients()
		return err
	}

	// Kill CometBFT if the ABCI application crashes.
	go conn.killTMOnClientError()

	return nil
}

func (conn *multiplexAppConn) startQueryClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.query == nil {
		c, err := conn.clientCreator.NewABCIQueryClient()
		if err != nil {
			return fmt.Errorf("error creating ABCI client (query client): %w", err)
		}
		conn.sharedClients.query = c
		shouldStart = true
	}

	// .. But we create x connections with the client, one per replicated chain
	for _, chainId := range conn.chainIds {
		conn.connsMutex.Lock()
		conn.queryConns[chainId] = NewChainConnQuery(chainId, conn.sharedClients.query, conn.metrics)
		conn.connsMutex.Unlock()
	}

	// Start only on thread that creates the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.query, "query")
	}

	return nil
}

func (conn *multiplexAppConn) startSnapshotClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.snapshot == nil {
		c, err := conn.clientCreator.NewABCISnapshotClient()
		if err != nil {
			return fmt.Errorf("error creating ABCI client (snapshot client): %w", err)
		}
		conn.sharedClients.snapshot = c
		shouldStart = true
	}

	// .. But we create x connections with the client, one per replicated chain
	for _, chainId := range conn.chainIds {
		conn.connsMutex.Lock()
		conn.snapshotConns[chainId] = NewChainConnSnapshot(chainId, conn.sharedClients.snapshot, conn.metrics)
		conn.connsMutex.Unlock()
	}

	// Start only on thread that created the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.snapshot, "snapshot")
	}

	return nil
}

func (conn *multiplexAppConn) startMempoolClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.mempool == nil {
		c, err := conn.clientCreator.NewABCIMempoolClient()
		if err != nil {
			return fmt.Errorf("error creating ABCI client (mempool client): %w", err)
		}
		conn.sharedClients.mempool = c
		shouldStart = true
	}

	// .. But we create x connections with the client, one per replicated chain
	for _, chainId := range conn.chainIds {
		conn.connsMutex.Lock()
		conn.mempoolConns[chainId] = NewChainConnMempool(chainId, conn.sharedClients.mempool, conn.metrics)
		conn.connsMutex.Unlock()
	}

	// Start only on thread that created the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.mempool, "mempool")
	}

	return nil
}

func (conn *multiplexAppConn) startConsensusClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.consensus == nil {
		c, err := conn.clientCreator.NewABCIConsensusClient()
		if err != nil {
			conn.stopAllClients()
			return fmt.Errorf("error creating ABCI client (consensus client): %w", err)
		}
		conn.sharedClients.consensus = c
		shouldStart = true
	}

	// .. But we create x connections with the client, one per replicated chain
	for _, chainId := range conn.chainIds {
		conn.connsMutex.Lock()
		conn.consensusConns[chainId] = NewChainConnConsensus(chainId, conn.sharedClients.consensus, conn.metrics)
		conn.connsMutex.Unlock()
	}

	// Start only on thread that created the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.consensus, "consensus")
	}

	return nil
}

func (conn *multiplexAppConn) startClient(c abcicli.Client, addr string) error {
	c.SetLogger(conn.Logger.With("module", "abci-client", "connection", addr))
	if err := c.Start(); err != nil {
		return fmt.Errorf("error starting ABCI client (%s client): %w", addr, err)
	}
	return nil
}

// OnStop implements [service.Service]
func (conn *multiplexAppConn) OnStop() {
	conn.stopAllClients()
}

func (conn *multiplexAppConn) killTMOnClientError() {
	killFn := func(conn string, err error, logger cmtlog.Logger) {
		logger.Error(
			conn+" connection terminated. Did the application crash? Please restart CometBFT",
			"err", err)
		killErr := cmtos.Kill()
		if killErr != nil {
			logger.Error("Failed to kill this process - please do so manually", "err", killErr)
		}
	}

	select {
	case <-conn.sharedClients.consensus.Quit():
		if err := conn.sharedClients.consensus.Error(); err != nil {
			killFn(connConsensus, err, conn.Logger)
		}
	case <-conn.sharedClients.mempool.Quit():
		if err := conn.sharedClients.mempool.Error(); err != nil {
			killFn(connMempool, err, conn.Logger)
		}
	case <-conn.sharedClients.query.Quit():
		if err := conn.sharedClients.query.Error(); err != nil {
			killFn(connQuery, err, conn.Logger)
		}
	case <-conn.sharedClients.snapshot.Quit():
		if err := conn.sharedClients.snapshot.Error(); err != nil {
			killFn(connSnapshot, err, conn.Logger)
		}
	}
}

func (conn *multiplexAppConn) stopAllClients() {
	if conn.sharedClients.consensus != nil {
		if err := conn.sharedClients.consensus.Stop(); err != nil {
			conn.Logger.Error("error while stopping consensus client", "error", err)
		}
	}
	if conn.sharedClients.mempool != nil {
		if err := conn.sharedClients.mempool.Stop(); err != nil {
			conn.Logger.Error("error while stopping mempool client", "error", err)
		}
	}
	if conn.sharedClients.query != nil {
		if err := conn.sharedClients.query.Stop(); err != nil {
			conn.Logger.Error("error while stopping query client", "error", err)
		}
	}
	if conn.sharedClients.snapshot != nil {
		if err := conn.sharedClients.snapshot.Stop(); err != nil {
			conn.Logger.Error("error while stopping snapshot client", "error", err)
		}
	}
}
