package snapsapp

import (
	"fmt"
	"path/filepath"
	"sync"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"

	"github.com/cometbft/cometbft/multiplex/snapshots"
)

const (
	// The AppVersion constant determines the current state machine version,
	// it can be increased to mark an upgrade in the State storage format.
	AppVersion = 1

	// The snapsappVersion constant determines the current compiled version
	// of this ABCI application.
	snapsappVersion = "snapsapp/v1"
)

// ----------------------------------------------------------------------------
// SnapsApp
//
// SnapsApp defines an ABCI application around a multiplex chain registry, and
// which delegates snapshotting to a [snapshots.Manager] implementation.
//
// This application creates snapshots of full state machines, without filtering
// any of the included properties: ChainID, ConsensusParams, Validators, etc.
//
// Read-write mutexes are created to track initial heights on concurrent
// threads, as well as for the currently working height in the process of
// finalizing and commiting blocks.
//
// Note that *only one instance* of the SnapsApp application must be created
// for node multiplexes. The SnapsApp application must be thread-safe and uses
// one [snapshots.Manager] instance per replicated chain.
type SnapsApp struct {
	// A logger instance to report asynchronous ABCI messages.
	logger cmtlog.Logger

	// A multiplex reactor as described with [Reactor].
	reactor Reactor

	// A map of [snapshots.Manager] instances mapped to ChainID values.
	snapshotManagers map[string]*snapshots.Manager

	// The current heights being worked on for replicated chains.
	chMutex        *sync.RWMutex
	currentHeights map[string]int64

	// The initial heights as used for state-sync of replicated chains.
	ihMutex        *sync.RWMutex
	initialHeights map[string]int64

	// The finalized block heights consist of working block heights.
	fbMutex              *sync.RWMutex
	finalizeBlockHeights map[string]int64
}

var _ abcitypes.Application = (*SnapsApp)(nil)

// NewSnapsApplication creates a [SnapsApp] ABCI application instance and
// initializes a [snapshots.Manager] for every replicated chain.
func NewSnapsApplication(
	reactor Reactor,
	snapshotOptions config.SnapshotOptions,
	logger cmtlog.Logger,
	options ...func(*SnapsApp),
) *SnapsApp {
	app := &SnapsApp{
		reactor: reactor,
		logger:  logger,
		chMutex: new(sync.RWMutex),
		ihMutex: new(sync.RWMutex),
		fbMutex: new(sync.RWMutex),
	}

	// Apply all options before anything else
	for _, option := range options {
		option(app)
	}

	// Use the chain registry to determine which chains are of interest
	replicatedChains := reactor.GetNetworks()
	storagePaths := reactor.GetStoragePaths()

	// initial heights are thread-safe
	app.ihMutex.Lock()
	app.initialHeights = make(map[string]int64, len(replicatedChains))
	app.ihMutex.Unlock()

	// working heights are thread-safe
	app.chMutex.Lock()
	app.currentHeights = make(map[string]int64, len(replicatedChains))
	app.chMutex.Unlock()

	// finalizeBlock heights must be thread-safe
	app.fbMutex.Lock()
	app.finalizeBlockHeights = make(map[string]int64, len(replicatedChains))
	app.fbMutex.Unlock()

	// Each replicated chain creates its own snapshot manager instance
	app.snapshotManagers = make(map[string]*snapshots.Manager, len(replicatedChains))
	for _, chainId := range replicatedChains {
		// Snapshots are stored in a different subfolder per chain
		// i.e.: %rootDir%/data/%address%/%ChainID%/snapshots/...
		chainDataFolder := storagePaths[chainId]
		snapshotsFolder := filepath.Join(chainDataFolder, "snapshots")

		// A snapshots store creates a `metadata.db` file and folders per-height
		snapshotStore, err := snapshots.NewStore(snapshotsFolder)
		if err != nil {
			panic(fmt.Errorf("could not create snapshots store: %w", err))
		}

		// Retrieve a particular chain's state machine store
		chainStore := reactor.GetStateStore(chainId)

		// The chain state machine implementation is passed as a commitment
		// snapshotter - which executes after a block is commited.
		// Snapshot() and Restore() are implemented in [ChainStateStore].
		manager := snapshots.NewManager(
			chainId,
			snapshotStore,
			snapshotOptions,
			chainStore,
			logger,
		)

		app.snapshotManagers[chainId] = manager
	}

	return app
}

// InitialHeight returns the initial block height for a chainId.
func (app *SnapsApp) InitialHeight(chainId string) int64 {
	app.ihMutex.RLock()
	defer app.ihMutex.RUnlock()

	return app.initialHeights[chainId]
}

// LastBlockHeight returns the last block height processed for a chainId.
func (app *SnapsApp) LastBlockHeight(chainId string) int64 {
	app.chMutex.RLock()
	defer app.chMutex.RUnlock()

	return app.currentHeights[chainId]
}

// FinalizeBlockHeight returns the latest finalizeBlock height
func (app *SnapsApp) FinalizeBlockHeight(chainId string) int64 {
	app.fbMutex.RLock()
	defer app.fbMutex.RUnlock()

	return app.finalizeBlockHeights[chainId]
}

func (app *SnapsApp) setFinalizeBlockHeight(chainId string, reqHeight int64) error {
	app.fbMutex.Lock()
	defer app.fbMutex.Unlock()

	app.finalizeBlockHeights[chainId] = reqHeight
	return nil
}
