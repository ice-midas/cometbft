package snapsapp

import (
	"fmt"
	"path/filepath"
	"sync"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"

	mx "github.com/cometbft/cometbft/multiplex"
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

	// A multiplex chain registry as defined with [mx.ChainRegistry].
	chainRegistry mx.ChainRegistry

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
	conf *config.Config,
	chainRegistry mx.ChainRegistry,
	multiplexStorage mx.MultiplexFS,
	multiplexState mx.MultiplexChainStore,
	logger cmtlog.Logger,
	options ...func(*SnapsApp),
) *SnapsApp {
	app := &SnapsApp{
		chainRegistry: chainRegistry,
		logger:        logger,
		chMutex:       new(sync.RWMutex),
		ihMutex:       new(sync.RWMutex),
		fbMutex:       new(sync.RWMutex),
	}

	// Apply all options before anything else
	for _, option := range options {
		option(app)
	}

	// Support multiple replication strategies, each defining their own format.
	mode := conf.Strategy
	opts := conf.SnapshotOptions[mode]

	// Use the chain registry to determine which chains are of interest
	replicatedChains := chainRegistry.GetChains()

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
		chainDataFolder := multiplexStorage[chainId]
		snapshotsFolder := filepath.Join(chainDataFolder, "snapshots")

		// A snapshots store creates a `metadata.db` file and folders per-height
		snapshotStore, err := snapshots.NewStore(snapshotsFolder)
		if err != nil {
			panic(fmt.Errorf("could not create snapshots store: %w", err))
		}

		// Retrieve a particular chain's state machine store
		chainStore, err := mx.NewChainStateStore(multiplexState, chainId)
		if err != nil {
			panic(fmt.Errorf("could not retrieve chain store: %w", err))
		}

		// The chain state machine implementation is passed as a commitment
		// snapshotter - which executes after a block is commited.
		// Snapshot() and Restore() are implemented in [ChainStateStore].
		manager := snapshots.NewManager(
			chainId,
			snapshotStore,
			opts,
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

func (app *SnapsApp) validateFinalizeBlockHeight(chainId string, reqHeight int64) error {
	if reqHeight < 1 {
		return fmt.Errorf("invalid height: %d", reqHeight)
	}

	lastBlockHeight := app.LastBlockHeight(chainId)
	chainInitialHeight := app.InitialHeight(chainId)
	finalizeBlockHeight := app.FinalizeBlockHeight(chainId)

	// expectedHeight holds the expected height to validate
	var expectedHeight int64
	if finalizeBlockHeight == 0 && chainInitialHeight > 1 {
		// In this case, we're validating the first block of the chain, i.e no
		// previous commit. The height we're expecting is the initial height.
		expectedHeight = chainInitialHeight
	} else {
		// This case can mean two things:
		//
		// - Either there was already a previous commit in the store, in which
		// case we increment the version from there.
		// - Or there was no previous commit, in which case we start at version 1.
		expectedHeight = lastBlockHeight + 1
	}

	if reqHeight != expectedHeight {
		return fmt.Errorf("invalid height: %d; expected: %d", reqHeight, expectedHeight)
	}

	return nil
}
