package multiplex

import (
	"fmt"
	"os"
	"slices"

	"github.com/cometbft/cometbft/crypto/merkle"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	types "github.com/cometbft/cometbft/types"
)

// -----------------------------------------------------------------------------
// ChecksummedGenesisDocSet

// Core types for a blockchain genesis in network replication mode:
// - struct ChecksummedGenesisDocSet implements [node.IChecksummedGenesisDoc]
// - struct GenesisDocSet consists of a slice of [types.GenesisDoc]

// ChecksummedGenesisDocSet combines a GenesisDocSet together with its SHA256 checksum.
type ChecksummedGenesisDocSet struct {
	GenesisDocs    GenesisDocSet
	Sha256Checksum []byte
}

// DefaultGenesisDoc() implements IChecksummedGenesisDoc
func (c *ChecksummedGenesisDocSet) DefaultGenesisDoc() (*types.GenesisDoc, error) {
	if len(c.GenesisDocs) > 1 {
		return nil, fmt.Errorf("no default genesis doc available in network replication mode")
	}

	return &c.GenesisDocs[0], nil
}

// GenesisDocByChainID() implements IChecksummedGenesisDoc
// This method returns an error when the genesis doc cannot be
// found for the given chainId.
func (c *ChecksummedGenesisDocSet) GenesisDocByChainID(
	chainId string,
) (*types.GenesisDoc, error) {
	if doc, ok, _ := c.GenesisDocs.SearchGenesisDocByChainID(chainId); ok {
		return &doc, nil
	}

	return nil, fmt.Errorf("could not find genesis doc for ChainID %s", chainId)
}

// GetChecksum() implements IChecksummedGenesisDoc
func (c *ChecksummedGenesisDocSet) GetChecksum() []byte {
	return c.Sha256Checksum
}

// -----------------------------------------------------------------------------
// GenesisDocSet

// GenesisDocSet defines the initial conditions for a CometBFT node multiplex,
// in particular their validator set and ChainID.
type GenesisDocSet []types.GenesisDoc

// SaveAs is a utility method for saving GenesisDocSet as a JSON file
func (genDocSet GenesisDocSet) SaveAs(file string) error {
	genDocSetBytes, err := cmtjson.Marshal(genDocSet)
	if err != nil {
		return err
	}
	//fmt.Printf("docSetBytes: %s", genDocSetBytes)
	return cmtos.WriteFile(file, genDocSetBytes, 0644)
}

// ValidatorHash returns the Merkle root hash built using genesis doc's
// validator hashes (as leaves).
func (genDocSet GenesisDocSet) ValidatorHash() []byte {
	bzs := make([][]byte, len(genDocSet))
	for _, genDoc := range genDocSet {
		bzs = append(bzs, genDoc.ValidatorHash())
	}
	return merkle.HashFromByteSlices(bzs)
}

// ValidateAndComplete checks that all necessary fields are present.
func (genDocSet GenesisDocSet) ValidateAndComplete() error {
	if len(genDocSet) < 1 {
		return fmt.Errorf("encountered empty GenesisDocSet")
	}

	chains := map[string]bool{}
	for i, userGenDoc := range genDocSet {
		chainId := userGenDoc.ChainID

		if _, ok := chains[chainId]; ok {
			return fmt.Errorf("duplicate ChainID in the genesis file %s", chainId)
		}

		err := userGenDoc.ValidateAndComplete()

		if err != nil {
			return err
		}

		// Flag for uniqueness
		chains[chainId] = true
		genDocSet[i] = userGenDoc
	}

	return nil
}

// SearchGenesisDocByChainID starts a search for a GenesisDoc by its ChainID.
func (genDocSet GenesisDocSet) SearchGenesisDocByChainID(
	chainId string,
) (doc types.GenesisDoc, found bool, err error) {
	if idx := slices.IndexFunc(genDocSet, func(d types.GenesisDoc) bool {
		return d.ChainID == chainId
	}); idx > -1 {
		return genDocSet[idx], true, nil
	}

	return types.GenesisDoc{}, false, nil
}

//------------------------------------------------------------
// Providers

// GenesisDocSetFromJSON unmarshalls JSON data into a GenesisDocSet.
func GenesisDocSetFromJSON(jsonBlob []byte) (GenesisDocSet, error) {
	genDocSet := GenesisDocSet{}

	err := cmtjson.Unmarshal(jsonBlob, &genDocSet)
	if err != nil {
		return nil, err
	}

	if err := genDocSet.ValidateAndComplete(); err != nil {
		return nil, err
	}

	return genDocSet, err
}

// GenesisDocSetFromFile reads JSON data from a file and unmarshalls it into a GenesisDocSet.
func GenesisDocSetFromFile(genDocSetFile string) (GenesisDocSet, error) {
	jsonBlob, err := os.ReadFile(genDocSetFile)
	if err != nil {
		return nil, fmt.Errorf("couldn't read GenesisDocSet file: %w", err)
	}
	genDocSet, err := GenesisDocSetFromJSON(jsonBlob)
	if err != nil {
		return nil, fmt.Errorf("error reading GenesisDocSet at %s: %w", genDocSetFile, err)
	}
	return genDocSet, nil
}
