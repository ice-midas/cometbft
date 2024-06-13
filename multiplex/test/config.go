package test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/cometbft/cometbft/config"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmttest "github.com/cometbft/cometbft/internal/test"
)

func ResetTestRootMultiplex(testName string) *config.Config {
	return ResetTestRootMultiplexWithChainID(testName, "")
}

func ResetTestRootMultiplexWithChainID(
	testName string,
	chainID string,
) *config.Config {
	// Initialize PrivValidator using cmttest
	conf := cmttest.ResetTestRootWithChainID(testName, chainID)

	rootDir := os.TempDir() + fmt.Sprintf("/%s-%s_", chainID, testName)

	config.EnsureRoot(rootDir)

	baseConfig := config.DefaultBaseConfig()
	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)

	if !cmtos.FileExists(genesisFilePath) {
		if chainID == "" {
			chainID = cmttest.DefaultTestChainID
		}

		// Overwrite genesis to use genesis doc set vs. single genesis doc
		testGenesis := fmt.Sprintf(testUserGenesisFmt, chainID)
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	return conf
}

func ResetTestRootMultiplexWithChainIDAndScopes(
	testName string,
	chainID string,
	userScopes map[string][]string,
) *config.Config {
	conf := cmttest.ResetTestRootWithChainID(testName, chainID)
	baseConfig := config.MultiplexBaseConfig(
		config.PluralReplicationMode(),
		userScopes,
	)

	// XXX:
	// This is not optimal as it is possible that undesired config will
	// be overwritten ; at best we should replace the EnsureRoot call
	// completely with this one. This solution is for further compat.
	config.EnsureRootMultiplex(conf.RootDir, &baseConfig)

	conf.UserConfig = baseConfig.UserConfig
	return conf
}

// TestScopeHash is a SHA256 of "user_address:scope" with TestUserAddress and scope "Default"
var TestScopeHash = "1A63C0E60122F9BB3DF9EA61C8184ED59C3936CAD1A3B35FA1D3BCA272E3F38B"
var TestUserAddress = "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
var testUserGenesisFmt = `[
	{
		"user_address": "%s",
		"genesis": {
			"genesis_time": "2018-10-10T08:20:13.695936996Z",
			"chain_id": "%s",
			"initial_height": "1",
			"consensus_params": {
				"block": {
					"max_bytes": "22020096",
					"max_gas": "-1",
					"time_iota_ms": "10"
				},
				"synchrony": {
					"message_delay": "500000000",
					"precision": "10000000"
				},
				"evidence": {
					"max_age_num_blocks": "100000",
					"max_age_duration": "172800000000000",
					"max_bytes": "1048576"
				},
				"validator": {
					"pub_key_types": [
						"ed25519"
					]
				},
				"abci": {
					"vote_extensions_enable_height": "0"
				},
				"version": {},
				"feature": {
					"vote_extensions_enable_height": "0",
					"pbts_enable_height": "1"
				}
			},
			"validators": [
				{
					"pub_key": {
						"type": "tendermint/PubKeyEd25519",
						"value":"AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="
					},
					"power": "10",
					"name": ""
				}
			],
			"app_hash": ""
		}
	}
]`
