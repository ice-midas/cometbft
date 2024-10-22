package multiplex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cometbft/cometbft/config"
)

func TestMultiplexConfigDefaultLegacyFallback(t *testing.T) {
	// Must use EmptyMultiplexConfig()
	conf := config.TestConfig()
	assert.Equal(t, config.DefaultReplicationStrategy(), conf.Strategy)

	// Must use EmptyMultiplexConfig()
	baseConf := config.DefaultBaseConfig()
	assert.Equal(t, config.DefaultReplicationStrategy(), baseConf.Strategy)
}

func TestMultiplexConfigMultiplexBaseConfig(t *testing.T) {
	// Must detect empty multiplex config
	conf := config.MultiplexBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{},
	)
	assert.Equal(t, config.DefaultReplicationStrategy(), conf.Strategy)

	// Must accept chainSeeds
	conf = config.MultiplexBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "id@host:port",
		},
		map[string][]string{},
	)
	assert.NotEqual(t, config.DefaultReplicationStrategy(), conf.Strategy)
	assert.NotEmpty(t, conf.ChainSeeds)

	// Must accept userChains
	conf = config.MultiplexBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
			},
		},
	)
	assert.NotEqual(t, config.DefaultReplicationStrategy(), conf.Strategy)
	assert.NotEmpty(t, conf.UserChains)
}
