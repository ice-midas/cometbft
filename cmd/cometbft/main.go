package main

import (
	"os"
	"path/filepath"

	cmd "github.com/ice-blockchain/cometbft/cmd/cometbft/commands"
	"github.com/ice-blockchain/cometbft/cmd/cometbft/commands/debug"
	cfg "github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/cli"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	nm "github.com/ice-blockchain/cometbft/node"
)

func main() {
	rootCmd := cmd.RootCmd
	rootCmd.AddCommand(
		cmd.GenValidatorCmd,
		cmd.InitFilesCmd,
		cmd.LightCmd,
		cmd.ResetAllCmd,
		cmd.ResetPrivValidatorCmd,
		cmd.ResetStateCmd,
		cmd.ShowValidatorCmd,
		cmd.TestnetFilesCmd,
		cmd.ShowNodeIDCmd,
		cmd.GenNodeKeyCmd,
		cmd.VersionCmd,
		cmd.RollbackStateCmd,
		cmd.CompactGoLevelDBCmd,
		cmd.InspectCmd,
		debug.DebugCmd,
		cli.NewCompletionCmd(rootCmd, true),
	)

	// NOTE:
	// Users wishing to:
	//	* Use an external signer for their validators
	//	* Supply an in-proc abci app
	//	* Supply a genesis doc file from another source
	//	* Provide their own DB implementation
	// can copy this file and use something other than the
	// DefaultNewNode function
	nodeFunc := nm.DefaultNewNode

	// Create & start node
	rootCmd.AddCommand(cmd.NewRunNodeCmd(nodeFunc))

	// Create & start nodes multiplex
	mxNodeFunc := mx.DefaultNewNodesMultiplex
	rootCmd.AddCommand(cmd.NewRunMultiplexCmd(mxNodeFunc))

	cmd := cli.PrepareBaseCmd(rootCmd, "CMT", os.ExpandEnv(filepath.Join("$HOME", cfg.DefaultCometDir)))
	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
