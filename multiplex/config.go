package multiplex

import (
	cmtcfg "github.com/cometbft/cometbft/config"
)

// ScopedUserConfig embeds UserConfig and computes SHA256 hashes
// by pairing user addresses and individual scopes such that every
// combination of user address and scope can be referred to by hash
type ScopedUserConfig struct {
	cmtcfg.UserConfig

	// userAddresses contains addresses (20 bytes hex) by which state is replicated.
	// This property is populated automatically and should not be modified.
	userAddresses []string

	// userScopeHashes maps user addresses to the complete list of SHA256 hashes
	// computed from pairing said user address with the scopes as listed above.
	// This property is populated automatically and should not be modified.
	userScopeHashes map[string][]string
}

// computeUserScopeHashes uses DefaultScopeHashProvider to populate the config
// instance's userScopeHashes and userAddresses fields.
func (c *ScopedUserConfig) computeUserScopeHashes() error {
	if c.Replication == cmtcfg.SingularReplicationMode() {
		return nil
	}

	c.userScopeHashes = map[string][]string{}
	if scopeRegistry, err := DefaultScopeHashProvider(&c.UserConfig); err == nil {
		c.userScopeHashes = scopeRegistry.ScopeHashes
	} else {
		return err
	}

	c.userAddresses = make([]string, len(c.UserScopes))
	for userAddress := range c.UserScopes {
		c.userAddresses = append(c.userAddresses, userAddress)
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
	repl cmtcfg.DataReplicationConfig,
	userScopes map[string][]string,
	startListenPort int,
) *ScopedUserConfig {
	config := &ScopedUserConfig{
		UserConfig: cmtcfg.UserConfig{
			Replication: cmtcfg.PluralReplicationMode(),
			UserScopes:  userScopes,
			ListenPort:  startListenPort,
		},
	}

	config.computeUserScopeHashes()
	return config
}

// NewScopedUserConfig returns a scoped configuration for UserConfig.
func NewScopedUserConfig(userScopes map[string][]string, startListenPort int) *ScopedUserConfig {
	config := NewUserConfig(
		cmtcfg.PluralReplicationMode(),
		userScopes,
		startListenPort,
	)
	return config
}

// ----------------------------------------------------------------------------
// Testing builders

// TestUserConfig returns a basic user configuration for testing a CometBFT node.
func TestUserConfig() ScopedUserConfig {
	return ScopedUserConfig{
		UserConfig: cmtcfg.DefaultUserConfig(),
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
		cmtcfg.PluralReplicationMode(),
		configScopes,
		30001,
	)
	return *cfg
}
