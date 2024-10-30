package client

import "context"

// InjectSnapshotMutation defines a callback that returns a processed/mutated state
// machine bytes representation (raw data) as they are snapshotted.
//
// We provide an example [DefaultSnapshotMutationExtension] implementation for the
// extensionFn parameter which only copies the state machine instance.
//
// This method is called by [multiplex.ChainStateStore] and may be used to
// mutate state instances *before* they are snapshotted and saved to disk.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultSnapshotMutationExtension].
func InjectSnapshotMutation(
	chainId string,
	stateBytes []byte,
	extensionFn SnapshotMutationExtensionFn,
) []byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [SnapshotMutationExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainId)
	chainContext := context.WithValue(context.TODO(), "Address", userAddress)
	chainContext = context.WithValue(chainContext, "ChainID", chainId)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the content of the state machine to a custom remote server
	// by implementing a custom SnapshotMutationExtensionFn, an example is
	// available with [DefaultSnapshotMutationExtension].
	nextStateBytes := extensionFn(chainContext, stateBytes)
	return nextStateBytes
}

// InjectPrepareProposal defines a callback that returns a pre-processed slice
// of transactions as they will be added to a block proposal.
//
// We provide an example [DefaultPrepareProposalExtension] implementation for the
// extensionFn parameter which only copies the transaction bytes.
//
// This method is called by [snapsapp.PrepareProposal] and may be used to
// pre-process or discard transactions before they are added to a proposal.
//
// Note that we inject `ChainID` in the Context before calling the proposed
// extensionFn callback, you can use these in your extension as documented
// with [DefaultPrepareProposalExtension].
func InjectPrepareProposal(
	chainId string,
	transactionsData [][]byte,
	extensionFn PrepareProposalExtensionFn,
) [][]byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [PrepareProposalExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainId)
	chainContext := context.WithValue(context.TODO(), "Address", userAddress)
	chainContext = context.WithValue(chainContext, "ChainID", chainId)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the transactions slice to a custom remote server
	// by implementing a custom PrepareProposalExtensionFn, an example is
	// available with [DefaultPrepareProposalExtension].
	nextTransactions := extensionFn(chainContext, transactionsData)
	return nextTransactions
}

// InjectFinalizeBlock defines a callback that returns a post-processed slice
// of transactions as they will be added to a finalized block.
//
// We provide an example [DefaultFinalizeBlockExtension] implementation for the
// extensionFn parameter which only copies the transaction bytes.
//
// This method is called by [snapsapp.FinalizeBlock] and may be used to
// post-process transactions as they are added to a finalized block and events.
//
// Note that we inject `ChainID` in the Context before calling the proposed
// extensionFn callback, you can use these in your extension as documented
// with [DefaultFinalizeBlockExtension].
func InjectFinalizeBlock(
	chainId string,
	transactionsData [][]byte,
	extensionFn FinalizeBlockExtensionFn,
) [][]byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [FinalizeBlockExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainId)
	chainContext := context.WithValue(context.TODO(), "Address", userAddress)
	chainContext = context.WithValue(chainContext, "ChainID", chainId)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the transactions slice to a custom remote server
	// by implementing a custom FinalizeBlockExtensionFn, an example is
	// available with [DefaultFinalizeBlockExtension].
	nextTransactions := extensionFn(chainContext, transactionsData)
	return nextTransactions
}
