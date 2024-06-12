package multiplex

import (
	"errors"
	"fmt"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtbytes "github.com/cometbft/cometbft/libs/bytes"
)

// XXX naming convention change, Multiplex* should be used only for maps
// MultiplexUserConfig embeds UserConfig and computes SHA256 hashes
// by pairing user addresses and individual scopes such that every
// combination of user address and scope can be referred to by hash
type MultiplexUserConfig struct {
	cfg.UserConfig

	// userAddresses contains addresses (20 bytes hex) by which state is replicated.
	// This property is populated automatically and should not be modified.
	userAddresses []string

	// userScopeHashes maps user addresses to the complete list of SHA256 hashes
	// computed from pairing said user address with the scopes as listed above.
	// This property is populated automatically and should not be modified.
	userScopeHashes map[string][]string
}

func (c *MultiplexUserConfig) computeUserScopeHashes() error {
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

func (c *MultiplexUserConfig) ScopeHashesByUser(userAddress string) []string {
	if hashes, ok := c.userScopeHashes[userAddress]; ok {
		return hashes
	}

	return []string{}
}

func (c *MultiplexUserConfig) ScopeHashes() []string {
	var allHashes []string
	for _, scopeHashes := range c.userScopeHashes {
		allHashes = append(allHashes, scopeHashes...)
	}

	return allHashes
}

// NewUserConfig returns a new pointer to a MultiplexUserConfig object.
func NewUserConfig(
	repl cfg.DataReplicationConfig,
	userScopes map[string][]string,
) *MultiplexUserConfig {
	cfg := &MultiplexUserConfig{
		UserConfig: cfg.UserConfig{
			Replication: repl,
			UserScopes:  userScopes,
		},
	}

	cfg.computeUserScopeHashes()
	return cfg
}

// PluralUserConfig returns a plural replication configuration for a CometBFT node.
func PluralUserConfig() MultiplexUserConfig {
	cfg := NewUserConfig(
		cfg.PluralReplicationMode(),
		map[string][]string{}, // empty scopes
	)
	return *cfg
}

// TestUserConfig returns a basic user configuration for testing a CometBFT node.
func TestUserConfig() MultiplexUserConfig {
	return MultiplexUserConfig{
		UserConfig: cfg.DefaultUserConfig(),
	}
}

// TestPluralUserConfig returns a plural replication configuration for testing a CometBFT node.
func TestPluralUserConfig(userScopes map[string][]string) MultiplexUserConfig {
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
