package snapsapp

import (
	"github.com/cometbft/cometbft/multiplex/client"
)

// GetPrepareProposalExtension returns the active data extension for transaction.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data processing unit, or extension.
func GetPrepareProposalExtension() client.PrepareProposalExtensionFn {
	return client.DefaultPrepareProposalExtension
}

// GetFinalizeBlockExtension returns the active data extension for finalized
// blocks transactions data.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data processing unit, or extension.
func GetFinalizeBlockExtension() client.FinalizeBlockExtensionFn {
	return client.DefaultFinalizeBlockExtension
}
