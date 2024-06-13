package multiplex

import (
	"errors"
	"fmt"
	"path/filepath"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtbytes "github.com/cometbft/cometbft/libs/bytes"
)

// ScopedUserConfig embeds UserConfig and computes SHA256 hashes
// by pairing user addresses and individual scopes such that every
// combination of user address and scope can be referred to by hash
type ScopedUserConfig struct {
	cfg.UserConfig

	// userAddresses contains addresses (20 bytes hex) by which state is replicated.
	// This property is populated automatically and should not be modified.
	userAddresses []string

	// userScopeHashes maps user addresses to the complete list of SHA256 hashes
	// computed from pairing said user address with the scopes as listed above.
	// This property is populated automatically and should not be modified.
	userScopeHashes map[string][]string
}

// computeUserScopeHashes computes computes SHA256 hashes by pairing user
// addresses and individual scopes (separated by `:`) such that every
// combination of user address and scope can be referred to by hash
func (c *ScopedUserConfig) computeUserScopeHashes() error {
	if c.Replication == cfg.SingularReplicationMode() {
		return nil
	}

	for userAddress, scopes := range c.UserScopes {
		// Create SHA256 hashes of the pair of user address and scope
		var userScopeHashes []string
		for _, scope := range scopes {
			scopeId := NewScopeID(userAddress, scope)
			userScopeHashes = append(userScopeHashes, scopeId.Hash())
		}

		// Builds a per-user slice of scope hashes
		c.userScopeHashes[userAddress] = userScopeHashes
		c.userAddresses = append(c.userAddresses, userAddress)
	}

	if c.Replication == cfg.PluralReplicationMode() && len(c.userScopeHashes) == 0 {
		return errors.New("in plural replication mode, at least one user scope is required")
	}

	return nil
}

// GetScopeHashes returns a slice of the flattened user scope hashes
func (c *ScopedUserConfig) GetScopeHashes() []string {
	var allHashes []string
	for _, scopeHashes := range c.userScopeHashes {
		allHashes = append(allHashes, scopeHashes...)
	}

	return allHashes
}

// ----------------------------------------------------------------------------
// Builders

// NewUserConfig returns a new pointer to a ScopedUserConfig object.
func NewUserConfig(
	repl cfg.DataReplicationConfig,
	userScopes map[string][]string,
) *ScopedUserConfig {
	config := &ScopedUserConfig{
		UserConfig: cfg.UserConfig{
			Replication: cfg.PluralReplicationMode(),
			UserScopes:  userScopes,
		},
	}

	config.computeUserScopeHashes()
	return config
}

// NewScopedUserConfig returns a scoped configuration for UserConfig.
func NewScopedUserConfig(userScopes map[string][]string) *ScopedUserConfig {
	config := NewUserConfig(
		cfg.PluralReplicationMode(),
		userScopes,
	)
	return config
}

// NewConsensusConfigWithWalFile returns configuration for the consensus service
// with a custom wal file path.
func NewConsensusConfigWithWalFile(walFile string) *cfg.ConsensusConfig {
	if len(walFile) == 0 {
		walFile = filepath.Join(cfg.DefaultDataDir, "cs.wal", "wal")
	}

	return &cfg.ConsensusConfig{
		WalPath:                          walFile,
		TimeoutPropose:                   3000 * time.Millisecond,
		TimeoutProposeDelta:              500 * time.Millisecond,
		TimeoutPrevote:                   1000 * time.Millisecond,
		TimeoutPrevoteDelta:              500 * time.Millisecond,
		TimeoutPrecommit:                 1000 * time.Millisecond,
		TimeoutPrecommitDelta:            500 * time.Millisecond,
		TimeoutCommit:                    1000 * time.Millisecond,
		SkipTimeoutCommit:                false,
		CreateEmptyBlocks:                true,
		CreateEmptyBlocksInterval:        0 * time.Second,
		PeerGossipSleepDuration:          100 * time.Millisecond,
		PeerQueryMaj23SleepDuration:      2000 * time.Millisecond,
		PeerGossipIntraloopSleepDuration: 0 * time.Second,
		DoubleSignCheckHeight:            int64(0),
	}
}

// ----------------------------------------------------------------------------
// Testing builders

// TestUserConfig returns a basic user configuration for testing a CometBFT node.
func TestUserConfig() ScopedUserConfig {
	return ScopedUserConfig{
		UserConfig: cfg.DefaultUserConfig(),
	}
}

// TestScopedUserConfig returns a scoped configuration for UserConfig.
func TestScopedUserConfig(userScopes map[string][]string) ScopedUserConfig {
	configScopes := make(map[string][]string, len(userScopes))
	if len(userScopes) == 0 {
		configScopes = map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default"},
		}
	} else {
		for userAddress, scopes := range userScopes {
			configScopes[userAddress] = scopes
		}
	}

	cfg := NewUserConfig(
		cfg.PluralReplicationMode(),
		configScopes,
	)
	return *cfg
}

// -----------------------------------------------------------------------------
// scopeID
// A private struct to deal with unique pairs of user address and scope.

const (
	fingerprintSize = 8
)

// scopeID embeds a string and adds a scope hash
type scopeID struct {
	ScopeHash string
	string
}

func (s scopeID) Hash() (sum string) {
	if len(s.ScopeHash) == 0 {
		sum256 := tmhash.Sum([]byte(s.string))
		s.ScopeHash = cmtbytes.HexBytes(sum256).String()
	}

	return s.ScopeHash
}

func (s scopeID) Fingerprint() (fp string) {
	if len(s.ScopeHash) == 0 {
		s.Hash()
	}

	hashBytes := cmtbytes.HexBytes([]byte(s.ScopeHash)[:fingerprintSize])
	return hashBytes.String()
}

func (s scopeID) String() string {
	return s.string
}

func NewScopeID(userAddress string, scope string) scopeID {
	return scopeID{
		string: fmt.Sprintf("%s:%s", userAddress, scope),
	}
}

func NewScopeIDFromHash(scopeHash string) scopeID {
	return scopeID{
		ScopeHash: scopeHash,
	}
}
