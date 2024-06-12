package multiplex

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"slices"

	dbm "github.com/cometbft/cometbft-db"
	cfg "github.com/cometbft/cometbft/config"
)

// ScopedDBContext embeds a cfg.DBContext and adds a scope hash
type ScopedDBContext struct {
	ScopeHash string
	cfg.DBContext
}

// ScopedDB embeds a dbm.DB and adds a scope hash
type ScopedDB struct {
	ScopeHash string
	dbm.DB
}

// XXX multiplex objects should be: type xMultiplex map[string]*x
type MultiplexDB []ScopedDB

// MultiplexDBProvider returns multiple databases using the DBBackend and DBDir
// specified in the Config and uses two levels of subfolders for user and purpose.
func MultiplexDBProvider(ctx *ScopedDBContext) (multiplex MultiplexDB, err error) {
	dbType := dbm.BackendType(ctx.Config.DBBackend)

	// XXX:
	// It may make more sense to use the scope fingerprint as a subfolder
	// for the database, including maybe a fingerprint of the user address.
	// The current two-level fs is easiest for testing and investigations.

	// Storage is located in scopes subfolders per each user
	for _, userAddress := range ctx.Config.GetAddresses() {
		// Uses one subfolder by user
		dbStorage := filepath.Join(ctx.Config.DBDir(), userAddress)

		for _, scope := range ctx.Config.UserScopes[userAddress] {
			// .. and one subfolder by scope
			dbStorage += filepath.Join(dbStorage, scope)

			userDb, err := dbm.NewDB(ctx.ID, dbType, dbStorage)
			if err != nil {
				return []ScopedDB{}, err
			}

			scopeID := NewScopeID(userAddress, scope)
			usDB := ScopedDB{
				ScopeHash: scopeID.Hash(),
				DB:        userDb,
			}

			multiplex = append(multiplex, usDB)
		}
	}

	return multiplex, nil
}

// GetScopedDB tries to find a DB in the multiplex using its scope hash
func GetScopedDB(
	multiplex MultiplexDB,
	userScopeHash string,
) (usDB ScopedDB, err error) {
	scopeHash := []byte(userScopeHash)
	if len(scopeHash) != sha256.Size {
		return ScopedDB{}, fmt.Errorf("incorrect scope hash for user scoped DB, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if idx := slices.IndexFunc(multiplex, func(db ScopedDB) bool {
		return db.ScopeHash == userScopeHash
	}); idx > -1 {
		return multiplex[idx], nil
	}

	return ScopedDB{}, fmt.Errorf("could not find user scoped DB using scope hash %s", scopeHash)
}
