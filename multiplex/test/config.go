package test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cometbft/cometbft/config"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmttest "github.com/cometbft/cometbft/internal/test"
)

func ResetTestRootMultiplexDisabled(testName string, chainID string) *config.Config {
	return cmttest.ResetTestRootWithChainID(testName, chainID)
}

func ResetTestRootMultiplexWithChainIDAndScopes(
	testName string,
	chainID string,
	userScopes map[string][]string,
) *config.Config {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", fmt.Sprintf("%s-%s_", chainID, testName))
	if err != nil {
		panic(err)
	}

	config.EnsureRoot(rootDir)

	baseConfig := config.MultiplexBaseConfig(
		config.PluralReplicationMode(),
		userScopes,
	)

	// XXX:
	// This is not optimal as it is possible that undesired config will
	// be overwritten ; at best we should replace the EnsureRoot call
	// completely with this one. This solution is for further compat.
	config.EnsureRootMultiplex(rootDir, &baseConfig)

	// IMPORTANT:
	// If there is a genesis file at the configured path, read it and expect it
	// to contain a genesis doc set ; otherwise create it with testUserGenesisFmt.
	// TODO: Right now, the chainID is forcefully overwritten using a loop ; it is
	// the network owner's responsibility to provide separate chainIDs.

	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		if chainID == "" {
			chainID = cmttest.DefaultTestChainID
		}
		var testGenesis string
		if len(userScopes) == 0 {
			testGenesis = fmt.Sprintf(testUserGenesisFmt, TestUserAddress, TestScope, chainID)
		} else {
			testGenesis = `[`
			for i, scopeDesc := range baseConfig.GetScopes() {
				parts := strings.Split(scopeDesc, ":")
				userAddress, scope := parts[0], parts[1]

				// Creates one genesis doc per pair of user address and scope
				perUserChainID := chainID + "-" + strconv.Itoa(i)
				scopedTestGenesis := fmt.Sprintf(testOneScopedGenesisFmt, userAddress, scope, perUserChainID)
				testGenesis += scopedTestGenesis + ","
			}

			// Removes last comma and closes json array
			testGenesis = testGenesis[:len(testGenesis)-1] + `]`
		}

		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// XXX TBI: put singular genesis docs inside scoped subfolders?

	// We always overwrite the priv val
	cmttest.ResetTestPrivValidator(rootDir, baseConfig)

	conf := config.MultiplexTestConfig(baseConfig.Replication, userScopes).SetRoot(rootDir)
	return conf
}

// TestScopeHash is a SHA256 of "user_address:scope" with TestUserAddress and scope "Default"
var TestScopeHash = "1A63C0E60122F9BB3DF9EA61C8184ED59C3936CAD1A3B35FA1D3BCA272E3F38B"
var TestFolderName = "1A63C0E60122F9BB" // fingerprint of TestScopeHash
var TestScope = "Default"
var TestUserAddress = "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
var testUserGenesisFmt = `[` + testOneScopedGenesisFmt + `]`
var testOneScopedGenesisFmt = `{
	"user_address": "%s",
	"scope": "%s",
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
}`
