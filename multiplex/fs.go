package multiplex

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"

	cfg "github.com/cometbft/cometbft/config"
)

// ----------------------------------------------------------------------------
// Multiplex providers

// MultiplexFSProvider returns multiple data filesystem paths using DefaultDataDir
// specified in the Config and uses two levels of subfolders for user and scope.
func MultiplexFSProvider(config *cfg.Config) (multiplex MultiplexFS, err error) {

	// When replication is in singular mode, we will create only one data dir
	// This mimics the default behaviour of CometBFT blockchain nodes' data dir
	if config.Replication == cfg.SingularReplicationMode() {
		multiplex = make(map[string]string, 1)
		multiplex[""] = cfg.DefaultDataDir
		return multiplex, nil
	}

	// XXX:
	// It may make more sense to use the scope fingerprint as a subfolder
	// for the database, including maybe a fingerprint of the user address.
	// The current two-level fs is easiest for testing and investigations.
	multiplex = map[string]string{}

	// Storage is located in scopes subfolders per each user
	baseDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)
	for _, userAddress := range config.GetAddresses() {
		// Uses one subfolder by user
		userDir := filepath.Join(baseDir, userAddress)

		for _, scope := range config.UserScopes[userAddress] {
			// .. and one subfolder by scope
			scopedDir := filepath.Join(userDir, scope)
			scopeID := NewScopeID(userAddress, scope)
			scopeHash := scopeID.Hash()
			multiplex[scopeHash] = scopedDir

			// Any error here means the directory is not accessible
			if _, err := os.Stat(scopedDir); err != nil {
				return nil, fmt.Errorf("missing mandatory folder %s: %w", scopedDir, err)
			}
		}
	}

	return multiplex, nil
}

// ----------------------------------------------------------------------------
// Scoped instance getters

// GetScopedFS tries to find a filesystem path in the FS multiplex using its scope hash
// If an error is produced, the default data dir will be returned alongside the error
func GetScopedFS(
	multiplex MultiplexFS,
	userScopeHash string,
) (string, error) {
	scopeHash := []byte(userScopeHash)
	if len(scopeHash) != sha256.Size {
		return cfg.DefaultDataDir, fmt.Errorf("incorrect scope hash for state multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if scopedStoragePath, ok := multiplex[userScopeHash]; ok {
		return scopedStoragePath, nil
	}

	return cfg.DefaultDataDir, fmt.Errorf("could not find state in multiplex using scope hash %s", scopeHash)
}
