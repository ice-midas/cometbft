package snapsapp

import (
	"context"
	"errors"
	"fmt"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	snapshottypes "github.com/cometbft/cometbft/multiplex/snapshots/types"
)

// InitChain initializes the application's state and sets up the initial
// validator set and other consensus parameters.
//
// This method is called once before applying the genesis block and permits
// to overwrite an application's validator set and consensus params, as well
// as to set an initial state to be replicated.
//
// InitChain implements [abcitypes.Application]
func (app *SnapsApp) InitChain(
	ctx context.Context,
	req *abcitypes.InitChainRequest,
) (*abcitypes.InitChainResponse, error) {
	// Retrieve ChainID from request for InitChain
	chainId := req.ChainId

	// Make sure we handle only relevant snapshotting routines
	if !app.chainRegistry.HasChain(chainId) {
		return nil, fmt.Errorf("invalid chain-id on InitChain: %s is not replicated", chainId)
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainId]; !ok {
		return &abcitypes.InitChainResponse{}, nil
	}

	// On a new chain, we consider the init chain block height as 0, even though
	// req.InitialHeight is 1 by default.
	app.ihMutex.Lock()
	app.initialHeights[chainId] = req.InitialHeight
	if app.initialHeights[chainId] == 0 { // If initial height is 0, set it to 1
		app.initialHeights[chainId] = 1
	}
	app.ihMutex.Unlock()

	app.setFinalizeBlockHeight(chainId, 0)

	// if req.InitialHeight is > 1, then we set the initial version on the app
	app.chMutex.Lock()
	if req.InitialHeight > 1 {
		app.currentHeights[chainId] = req.InitialHeight
	} else {
		app.currentHeights[chainId] = 0
	}
	app.chMutex.Unlock()
	app.logger.Info("InitChain", "initialHeight", req.InitialHeight, "chainID", req.ChainId)

	// Get the commitment snapshotter instance to retrieve current AppHash
	snapshotter := app.snapshotManagers[req.ChainId].GetSnapshotter()

	// NOTE: We don't commit, but FinalizeBlock for block InitialHeight starts from
	// this FinalizeBlockState.
	return &abcitypes.InitChainResponse{
		ConsensusParams: req.ConsensusParams,
		Validators:      req.Validators,
		AppHash:         snapshotter.AppHash(),
	}, nil
}

// Info returns information about the application, including the AppHash.
//
// This method is called upon crash-recovery and after executing state-sync
// to verify the loaded AppHash.
//
// Info implements [abcitypes.Application]
func (app *SnapsApp) Info(
	ctx context.Context,
	req *abcitypes.InfoRequest,
) (*abcitypes.InfoResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	// Make sure we handle only relevant info requests
	if !app.chainRegistry.HasChain(chainId) {
		app.logger.Error("received irrelevant snapshot chain identifier (Info)", "chain_id", chainId)
		return &abcitypes.InfoResponse{}, nil
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainId]; !ok {
		app.logger.Error("snapshot manager not configured (Info)", "chain_id", chainId)
		return &abcitypes.InfoResponse{}, nil
	}

	snapshotter := app.snapshotManagers[chainId].GetSnapshotter()
	stateMachine, err := snapshotter.GetStateMachine()
	if err != nil {
		app.logger.Error("could not load state machine (Info)", "chain_id", chainId)
		return &abcitypes.InfoResponse{}, nil
	}

	// The ChainID is included in the response data
	return &abcitypes.InfoResponse{
		Data:             chainId,
		Version:          snapsappVersion,
		AppVersion:       AppVersion,
		LastBlockHeight:  stateMachine.LastBlockHeight,
		LastBlockAppHash: stateMachine.AppHash,
	}, nil
}

// CheckTx allows the application to validate transactions and/or discard them.
//
// This method may execute transactions in CheckTx mode, i.e. not actually
// executing messages. Note also that expensive operations should not be run
// here but rather in the commitment stage.
//
// TODO(midas): CheckTx not yet supported as of v1, all transactions are valid.
// CheckTx implements [abcitypes.Application]
func (app *SnapsApp) CheckTx(context.Context, *abcitypes.CheckTxRequest) (*abcitypes.CheckTxResponse, error) {
	return &abcitypes.CheckTxResponse{Code: abcitypes.CodeTypeOK}, nil
}

// Commit may persist the application state if any data is relevant and it must
// also determine whether a snapshot must be created, or not.
//
// This method is called after finalizing blocks. This method uses the snapshot
// manager to determine whether a new snapshot must be taken or not, based on
// the `interval` set in the [config.SnapshotOptions] instance for this chain.
//
// Commit implements [abcitypes.Application]
func (app *SnapsApp) Commit(
	ctx context.Context,
	req *abcitypes.CommitRequest,
) (*abcitypes.CommitResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	resp := &abcitypes.CommitResponse{
		RetainHeight: 0, // pruning is disabled, so block retention 0.
	}

	// Make sure we handle only relevant commits
	if !app.chainRegistry.HasChain(chainId) {
		app.logger.Error("received irrelevant snapshot chain identifier (Commit)", "chain_id", chainId)
		return resp, nil
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainId]; !ok {
		app.logger.Error("snapshot manager not configured (Info)", "chain_id", chainId)
		return resp, nil
	}

	app.fbMutex.RLock()
	workingHeight := app.finalizeBlockHeights[chainId]
	app.fbMutex.RUnlock()

	app.snapshotManagers[chainId].SnapshotIfApplicable(workingHeight)
	return resp, nil
}

// TODO(midas): Query not yet supported as of v1
// Query implements [abcitypes.Application]
func (app *SnapsApp) Query(context.Context, *abcitypes.QueryRequest) (*abcitypes.QueryResponse, error) {
	return &abcitypes.QueryResponse{Code: abcitypes.CodeTypeOK}, nil
}

// ListSnapshots delegates to the correct snapshot manager to list recent
// snapshots for the requested replicated chain.
//
// This method is called when a peer requests for snapshots. Note that the
// response includes only snapshot metadata, not the snapshot chunks.
//
// ListSnapshots implements [abcitypes.Application]
func (app *SnapsApp) ListSnapshots(
	ctx context.Context,
	req *abcitypes.ListSnapshotsRequest,
) (*abcitypes.ListSnapshotsResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	resp := &abcitypes.ListSnapshotsResponse{Snapshots: []*abcitypes.Snapshot{}}

	// Make sure we handle only relevant snapshotting routines
	if !app.chainRegistry.HasChain(chainId) {
		return nil, fmt.Errorf("invalid chain-id on ListSnapshots: %s is not replicated", chainId)
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainId]; !ok {
		app.logger.Error("snapshot manager not configured (ListSnapshots)", "chain_id", chainId)
		return resp, nil
	}

	// Read recent snapshots from filesystem
	snapshots, err := app.snapshotManagers[chainId].List()
	if err != nil {
		app.logger.Error("failed to list snapshots", "chain_id", chainId, "err", err)
		return nil, err
	}

	// Marshal snapshots for ABCI transport
	for _, snapshot := range snapshots {
		abciSnapshot, err := snapshot.ToABCI()
		if err != nil {
			app.logger.Error("failed to convert ABCI snapshots", "chain_id", chainId, "err", err)
			return nil, err
		}

		resp.Snapshots = append(resp.Snapshots, &abciSnapshot)
	}

	return resp, nil
}

// OfferSnapshot unmarshals snapshot metadata and starts the restoration of
// snapshot chunks (download).
//
// This method is called when a peer received a list of snapshots and chooses
// one to sync. It will initiate the download of snapshot chunks and restore
// the snapshot data. Note that this method does not *apply* the snapshot.
//
// ListSnapshots implements [abcitypes.Application]
func (app *SnapsApp) OfferSnapshot(
	ctx context.Context,
	req *abcitypes.OfferSnapshotRequest,
) (*abcitypes.OfferSnapshotResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	// Make sure we handle only relevant snapshotting routines
	if !app.chainRegistry.HasChain(chainId) {
		app.logger.Error("received irrelevant snapshot chain identifier (OfferSnapshot)", "chain_id", chainId)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ABORT}, nil
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainId]; !ok {
		app.logger.Error("snapshot manager not configured (OfferSnapshot)", "chain_id", chainId)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ABORT}, nil
	}

	// Obviously we do not permit nil-snapshots
	if req.Snapshot == nil {
		app.logger.Error("received nil snapshot", "chain_id", chainId)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT}, nil
	}

	// Unmarshal snapshots from ABCI transport
	snapshot, err := snapshottypes.SnapshotFromABCI(req.Snapshot)
	if err != nil {
		app.logger.Error("failed to decode snapshot metadata", "chain_id", chainId, "err", err)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT}, nil
	}

	// Verifies snapshot format and start downloading chunks
	err = app.snapshotManagers[chainId].Restore(snapshot)
	switch {
	case err == nil:
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ACCEPT}, nil

	case errors.Is(err, snapshottypes.ErrUnknownFormat):
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT_FORMAT}, nil

	case errors.Is(err, snapshottypes.ErrInvalidMetadata):
		app.logger.Error(
			"rejecting invalid snapshot",
			"chain_id", chainId,
			"height", req.Snapshot.Height,
			"format", req.Snapshot.Format,
			"err", err,
		)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT}, nil

	default:
		// CometBFT errors are defined here: https://github.com/cometbft/cometbft/blob/main/statesync/syncer.go
		// It may happen that in case of a CometBFT error, such as a timeout (which occurs after two minutes),
		// the process is aborted. This is done intentionally because deleting the database programmatically
		// can lead to more complicated situations.
		app.logger.Error(
			"failed to restore snapshot",
			"chain_id", chainId,
			"height", req.Snapshot.Height,
			"format", req.Snapshot.Format,
			"err", err,
		)

		// We currently don't support resetting the IAVL stores and retrying a
		// different snapshot, so we ask CometBFT to abort all snapshot restoration.
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ABORT}, nil
	}
}

// LoadSnapshotChunk delegates to the correct snapshot manager to load a chunk
// from the filesystem.
//
// This method is called to retrieve snapshot chunks which may be transported
// to other peers, i.e. asynchronously called when a peer downloads chunks.
//
// LoadSnapshotChunk implements [abcitypes.Application]
func (app *SnapsApp) LoadSnapshotChunk(
	ctx context.Context,
	req *abcitypes.LoadSnapshotChunkRequest,
) (*abcitypes.LoadSnapshotChunkResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	// Make sure we handle only relevant snapshotting routines
	if !app.chainRegistry.HasChain(chainId) {
		return nil, fmt.Errorf("invalid chain-id on ListSnapshots: %s is not replicated", chainId)
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainId]; !ok {
		return &abcitypes.LoadSnapshotChunkResponse{}, nil
	}

	chunk, err := app.snapshotManagers[chainId].LoadChunk(req.Height, req.Format, req.Chunk)
	if err != nil {
		app.logger.Error(
			"failed to load snapshot chunk",
			"chain_id", chainId,
			"height", req.Height,
			"format", req.Format,
			"chunk", req.Chunk,
			"err", err,
		)
		return nil, err
	}

	return &abcitypes.LoadSnapshotChunkResponse{Chunk: chunk}, nil
}

// ApplySnapshotChunk applies snapshot chunks sequentially.
//
// This method is called when a peer received a snapshot chunk (downloaded).
// The downloaded snapshot chunk will be applied with this method, thus
// updating the filesystem. When all snapshot chunks have finished downloading,
// the snapshot will be considered fully applied.
//
// ApplySnapshotChunk implements [abcitypes.Application]
func (app *SnapsApp) ApplySnapshotChunk(
	ctx context.Context,
	req *abcitypes.ApplySnapshotChunkRequest,
) (*abcitypes.ApplySnapshotChunkResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	// Make sure we handle only relevant snapshotting routines
	if !app.chainRegistry.HasChain(chainId) {
		app.logger.Error("received irrelevant snapshot chain identifier (ApplySnapshotChunk)", "chain_id", chainId)
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ABORT}, nil
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainId]; !ok {
		app.logger.Error("snapshot manager not configured (ApplySnapshotChunk)", "chain_id", chainId)
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ABORT}, nil
	}

	// Applies snapshot chunk in order (updates filesystem)
	_, err := app.snapshotManagers[chainId].RestoreChunk(req.Chunk)
	switch {
	case err == nil:
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ACCEPT}, nil

	case errors.Is(err, snapshottypes.ErrChunkHashMismatch):
		app.logger.Error(
			"chunk checksum mismatch; rejecting sender and requesting refetch",
			"chain_id", chainId,
			"chunk", req.Index,
			"sender", req.Sender,
			"err", err,
		)
		return &abcitypes.ApplySnapshotChunkResponse{
			Result:        abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_RETRY,
			RefetchChunks: []uint32{req.Index},
			RejectSenders: []string{req.Sender},
		}, nil

	default:
		app.logger.Error("failed to restore snapshot", "chain_id", chainId, "err", err)
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ABORT}, nil
	}
}

// PrepareProposal implements the PrepareProposal ABCI method and returns a
// ResponsePrepareProposal object to the client. The PrepareProposal method is
// responsible for allowing the block proposer to perform application-dependent
// work in a block before proposing it.
//
// Transactions can be modified, removed, or added by the application. Since the
// application maintains its own local mempool, it will ignore the transactions
// provided to it in RequestPrepareProposal. Instead, it will determine which
// transactions to return based on the mempool's semantics and the MaxTxBytes
// provided by the client's request.
//
// PrepareProposal implements [abcitypes.Application]
func (app *SnapsApp) PrepareProposal(
	_ context.Context,
	req *abcitypes.PrepareProposalRequest,
) (*abcitypes.PrepareProposalResponse, error) {
	// CometBFT must never call PrepareProposal with a height of 0.
	//
	// Ref: https://github.com/cometbft/cometbft/blob/059798a4f5b0c9f52aa8655fa619054a0154088c/spec/core/state.md?plain=1#L37-L38
	if req.Height < 1 {
		return nil, errors.New("PrepareProposal called with invalid height")
	}

	txs := make([][]byte, 0, len(req.Txs))
	var totalBytes int64
	for _, tx := range req.Txs {
		totalBytes += int64(len(tx))
		if totalBytes > req.MaxTxBytes {
			break
		}
		txs = append(txs, tx)
	}
	return &abcitypes.PrepareProposalResponse{Txs: txs}, nil
}

// ProcessProposal implements the ProcessProposal ABCI method and returns a
// ResponseProcessProposal object to the client. The ProcessProposal method is
// responsible for allowing execution of application-dependent work in a proposed
// block. Note, the application defines the exact implementation details of
// ProcessProposal. In general, the application must at the very least ensure
// that all transactions are valid. If all transactions are valid, then we inform
// CometBFT that the Status is ACCEPT. However, the application is also able
// to implement optimizations such as executing the entire proposed block
// immediately.
//
// If a panic is detected during execution of an application's ProcessProposal
// handler, it will be recovered and we will reject the proposal.
//
// ProcessProposal implements [abcitypes.Application]
func (app *SnapsApp) ProcessProposal(
	ctx context.Context,
	req *abcitypes.ProcessProposalRequest,
) (*abcitypes.ProcessProposalResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	// Make sure we handle only relevant proposals
	if !app.chainRegistry.HasChain(chainId) {
		return nil, fmt.Errorf("received irrelevant chain identifier (ProcessProposal): %s", chainId)
	}

	// CometBFT must never call ProcessProposal with a height of 0.
	//
	// Ref: https://github.com/cometbft/cometbft/blob/059798a4f5b0c9f52aa8655fa619054a0154088c/spec/core/state.md?plain=1#L37-L38
	if req.Height < 1 {
		return nil, errors.New("ProcessProposal called with invalid height")
	}

	// Update the current working block height
	app.chMutex.Lock()
	app.currentHeights[chainId] = req.Height
	app.chMutex.Unlock()

	return &abcitypes.ProcessProposalResponse{Status: abcitypes.PROCESS_PROPOSAL_STATUS_ACCEPT}, nil
}

// ExtendVote implements the ExtendVote ABCI method and returns a ResponseExtendVote.
// It calls the application's ExtendVote handler which is responsible for performing
// application-specific business logic when sending a pre-commit for the NEXT
// block height. The extensions response may be non-deterministic but must always
// be returned, even if empty.
//
// Agreed upon vote extensions are made available to the proposer of the next
// height and are committed in the subsequent height, i.e. H+2. An error is
// returned if vote extensions are not enabled or if extendVote fails or panics.
//
// ExtendVote implements [abcitypes.Application]
func (app *SnapsApp) ExtendVote(context.Context, *abcitypes.ExtendVoteRequest) (*abcitypes.ExtendVoteResponse, error) {
	return &abcitypes.ExtendVoteResponse{}, nil
}

// VerifyVoteExtension implements the VerifyVoteExtension ABCI method and returns
// a ResponseVerifyVoteExtension. It calls the applications' VerifyVoteExtension
// handler which is responsible for performing application-specific business
// logic in verifying a vote extension from another validator during the pre-commit
// phase. The response MUST be deterministic. An error is returned if vote
// extensions are not enabled or if verifyVoteExt fails or panics.
// We highly recommend a size validation due to performance degradation,
// see more here https://docs.cometbft.com/v1.0/references/qa/cometbft-qa-38#vote-extensions-testbed
//
// VerifyVoteExtension implements [abcitypes.Application]
func (app *SnapsApp) VerifyVoteExtension(context.Context, *abcitypes.VerifyVoteExtensionRequest) (*abcitypes.VerifyVoteExtensionResponse, error) {
	return &abcitypes.VerifyVoteExtensionResponse{
		Status: abcitypes.VERIFY_VOTE_EXTENSION_STATUS_ACCEPT,
	}, nil
}

// FinalizeBlock will execute the block proposal provided by FinalizeBlockRequest.
//
// Currently we do not perform any operation with transactions, thus the
// transactions are all deemed to be "valid".
//
// The finalizeBlockHeights is updated for the relevant chain such that the
// subsequent Commit() ABCI with the same ChainID may know which *height* is
// being finalized. This height is used to determine whether a snapshot must
// be taken or not.
//
// FinalizeBlock implements [abcitypes.Application]
func (app *SnapsApp) FinalizeBlock(
	ctx context.Context,
	req *abcitypes.FinalizeBlockRequest,
) (*abcitypes.FinalizeBlockResponse, error) {
	// Retrieve ChainID from context
	chainId := ctx.Value("ChainID").(string)

	resp := &abcitypes.FinalizeBlockResponse{TxResults: []*abcitypes.ExecTxResult{}}

	// Make sure we handle only relevant snapshotting routines
	if !app.chainRegistry.HasChain(chainId) {
		return resp, fmt.Errorf("invalid chain-id on FinalizeBlock: %s is not replicated", chainId)
	}

	if err := app.validateFinalizeBlockHeight(chainId, req.Height); err != nil {
		return nil, err
	}

	txs := make([]*abcitypes.ExecTxResult, len(req.Txs))
	for i := range req.Txs {
		txs[i] = &abcitypes.ExecTxResult{Code: abcitypes.CodeTypeOK}
	}

	// We can now safely store the finalized block height
	app.setFinalizeBlockHeight(chainId, req.Height)

	return &abcitypes.FinalizeBlockResponse{
		TxResults: txs,
	}, nil
}
