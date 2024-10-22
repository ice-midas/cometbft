package multiplex

import (
	"github.com/cometbft/cometbft/config"
)

// -----------------------------------------------------------------------------
// ReplicationStrategy

// HistoryReplicationStrategy() returns the historical node type which uses a mode of
// "History", i.e. it does not synchronize with replicated chains.
func HistoryReplicationStrategy() config.ReplicationStrategy {
	return config.NewReplicationStrategy("History")
}

// NetworkReplicationStrategy() returns the replicator node type which uses a mode of
// "Network", i.e. it does synchronize with replicated chains.
func NetworkReplicationStrategy() config.ReplicationStrategy {
	return config.NewReplicationStrategy("Network")
}

// DisableReplicationStrategy() returns the legacy node type which uses a mode,
// i.e. it disables multiplex features and uses the legacy node implementation.
func DisableReplicationStrategy() config.ReplicationStrategy {
	return config.NewReplicationStrategy("Disable")
}

// -----------------------------------------------------------------------------
// Options helper implementations for [config.MultiplexConfig].

// WithStrategy is an option helper that allows you to overwrite the
// default Strategy in [MultiplexConfig].
// By default, this option is set to "Disable".
func WithStrategy(strategy config.ReplicationStrategy) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.Strategy = strategy
	}
}

// WithSyncConfig is an option helper that allows you to overwrite the
// default SyncConfig in [MultiplexConfig].
// By default, this option is set to an empty map.
func WithSyncConfig(syncConfigs map[string]*config.StateSyncConfig) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.SyncConfig = make(map[string]*config.StateSyncConfig, len(syncConfigs))
		for chainId, syncConfig := range syncConfigs {
			conf.SyncConfig[chainId] = syncConfig
		}
	}
}

// WithChainSeeds is an option helper that allows you to overwrite the
// default (empty) ChainSeeds in [MultiplexConfig].
// By default, this option is set to an empty map.
func WithChainSeeds(chainSeeds map[string]string) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.ChainSeeds = make(map[string]string, len(chainSeeds))
		for chainId, seedNodes := range chainSeeds {
			conf.ChainSeeds[chainId] = seedNodes
		}
	}
}

// WithUserChains is an option helper that allows you to overwrite the
// default (empty) UserChains in [MultiplexConfig].
// By default, this option is set to an empty map.
func WithUserChains(userChains map[string][]string) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.UserChains = map[string][]string{}
		for address, fingerprints := range userChains {
			conf.UserChains[address] = make([]string, len(fingerprints))
			for _, fp := range fingerprints {
				conf.UserChains[address] = append(conf.UserChains[address], fp)
			}
		}
	}
}

// WithP2PStartPort is an option helper that allows you to overwrite the
// default P2PStartPort in [MultiplexConfig].
// By default, this option is set to 30001.
func WithP2PStartPort(p2pStartPort uint16) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.P2PStartPort = p2pStartPort
	}
}

// WithRPCStartPort is an option helper that allows you to overwrite the
// default RPCStartPort in [MultiplexConfig].
// By default, this option is set to 40001.
func WithRPCStartPort(rpcStartPort uint16) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.RPCStartPort = rpcStartPort
	}
}
