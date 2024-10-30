package client

import (
	"regexp"
)

// extractAddressFromChainID returns the user address extracted from a ChainID.
func extractAddressFromChainID(chainId string) string {
	extractor := regexp.MustCompile(`(.*)\-([A-F0-9]+)\-([A-F0-9]+)`)
	matches := extractor.FindStringSubmatch(chainId)

	// Returns empty given non-compatible ChainID
	if len(matches) == 0 || len(matches) < 3 {
		return ""
	}

	// Returns the USER ADDRESS part of the ChainID
	return matches[2]
}
