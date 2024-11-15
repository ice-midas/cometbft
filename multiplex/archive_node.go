package multiplex

import (
	"context"
	"fmt"

	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/light"
	lightstore "github.com/cometbft/cometbft/light/store/db"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/proxy"

	"github.com/cometbft/cometbft/multiplex/snapsapp"
)

// ----------------------------------------------------------------------------
// NewHistoricalNode

// NewHistoricalNode returns a new, ready to go, historical node for CometBFT
// Nodes Multiplexes running multi-network replication strategies.
//
// Creates one [p2p.NodeKey] instance per historical node. Note also that this
// method does *not* call the Start() method for the created node instance.
//
// IMPORTANT:
// Historical nodes *do not* participate in consensus instances of the
// underlying blockchain networks. The node only connects to networks using a
// read-only adapter which downloads *historical data* snapshots from a nodes
// multiplex.
// Historical data snapshots use a different `Format` than network snapshots,
// and are not relevant to consensus instances. This format of snapshot cannot
// be used to bootstrap a new node and/or to connect to a network.
//
// CAUTION - EXPERIMENTAL:
// Running the following code is highly unrecommended in a production
// environment. Please use these features with caution as it is still
// being actively developed.
//
// See also: [NewNodesMultiplex], [NewLegacyNodeMultiplex]
func NewHistoricalNode(
	ctx context.Context,
	globalCfg *config.Config,
	logger cmtlog.Logger,
) (MultiplexMap[*light.Client], error) {
	// Creates one [p2p.NodeKey] instance per historical node
	nodeKey, err := p2p.LoadOrGenNodeKey(globalCfg.NodeKeyFile())
	if err != nil {
		return nil, fmt.Errorf("failed to load or generate node key %s: %w", globalCfg.NodeKeyFile(), err)
	}

	// Uses a singleton chain registry to interpret multiplex configurations
	chainRegistry, err := NewChainRegistry(&globalCfg.MultiplexConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load or generate node key %s: %w", globalCfg.NodeKeyFile(), err)
	}

	// Initialize a historical node reactor which handles the configuration
	// of state-sync for multiple networks.
	archive := NewArchive(
		nodeKey,
		globalCfg,
		logger.With("module", "history"),
		chainRegistry,
	)

	// Warn the user about experimental status
	logger.Info("WARNING - EXPERIMENTAL: Creating a historical node", "nodeId", string(nodeKey.ID()))

	if err := archive.Start(); err != nil {
		return nil, fmt.Errorf("could not create the historical node: %w", err)
	}

	// Create the local ABCI client for the SnapsApp application.
	//
	// This application is forcefully enabled using the multiplex package,
	// note that we also *ignore* the ProxyApp field in [config.Config].
	//
	// The ABCI client is created once for the historical node, and we use
	// a breaking [proxy.ChainConns] interface rather than [proxy.AppConns].
	snapshotsStrategy := globalCfg.SnapshotOptions[HistoryReplicationStrategy()]
	localABCISnapsApp := proxy.NewLocalClientCreator(snapsapp.NewSnapsApplication(
		archive,
		snapshotsStrategy,
		logger.With("module", "snapsapp"),
	))

	// Start the ABCI client (proxyApp)
	// Note that we create only one ABCI client shared by all replicated chains.
	//
	// BREAKING: we use [proxy.ChainConns] interfaces rather than [proxy.AppConns].
	abciClient := proxy.NewMultiplexAppConn(
		archive.GetNetworks(),
		localABCISnapsApp,
		proxy.PrometheusMetrics(globalCfg.Instrumentation.Namespace),
	)
	abciClient.SetLogger(logger.With("module", "proxy"))
	if err := abciClient.Start(); err != nil {
		return nil, fmt.Errorf("error starting proxy app connections: %w", err)
	}

	// Archive: ABCI; ABCI: Archive.
	archive.abciClient = abciClient

	// Setup state-sync reactors per network.
	if err := archive.initStateSync(); err != nil {
		return nil, fmt.Errorf("could not start the historical node: %w", err)
	}

	// Inform about all replicated chains being configured.
	logger.Info("Historical node is now configured", "nodeId", string(nodeKey.ID()))
	return archive.CreateHistoricalLightClients(ctx)
}

// ----------------------------------------------------------------------------
// Archive

func (archive *Archive) CreateHistoricalLightClients(
	ctx context.Context,
) (MultiplexMap[*light.Client], error) {
	lightClients := MultiplexMap[*light.Client]{}
	for _, chainId := range archive.GetNetworks() {
		// Requires state-sync configuration values
		// See also: [client.InjectSyncConfig]
		syncConfig, err := archive.chainRegistry.GetStateSyncConfig(chainId)
		if err != nil {
			return nil, err
		}

		// Requires a database per replicated chain
		clientStore := lightstore.NewWithDBVersion(
			archive.stateDatabases[chainId].DB,
			chainId,
			archive.nodeConfig.Storage.ExperimentalKeyLayout,
		)

		// Requires a minimum of 2 chain seeds to perform verifications
		networkNodes := syncConfig.RPCServers
		if len(networkNodes) < 2 {
			return nil, fmt.Errorf(
				"state-sync for historical node requires at least 2 network nodes")
		}

		client, err := light.NewHTTPClient(
			ctx,
			chainId,
			light.TrustOptions{
				Period: syncConfig.TrustPeriod,
				Height: syncConfig.TrustHeight,
				Hash:   syncConfig.TrustHashBytes(),
			},
			networkNodes[0],
			networkNodes[1:],
			clientStore,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"could not start a client for the historical node: %w", err)
		}

		lightClients[chainId] = NewChainInstance(chainId, client)
	}

	return lightClients, nil
}
