package multiplex

import (
	"crypto/sha256"
	"fmt"
	"slices"

	dbm "github.com/cometbft/cometbft-db"
	cfg "github.com/cometbft/cometbft/config"
)

// MultiplexDBContext specifies config information for loading a new DB.
type MultiplexDBContext struct {
	cfg.DBContext
	ScopeHash string
}

// UserScopedDB maps a dbm.DB to a pair of a user address and a scope
type UserScopedDB struct {
	UserAddress string
	Scope       string
	ScopeHash   string
	dbm.DB
}

type MultiplexDB []UserScopedDB

// GetScopeHash returns the ScopeHash value or generates it using scopeID
func (usDB *UserScopedDB) GetScopeHash() string {
	if len(usDB.ScopeHash) == 0 {
		scopeId := NewScopeID(usDB.UserAddress, usDB.Scope)
		usDB.ScopeHash = scopeId.Hash()
	}

	return usDB.ScopeHash
}

// MultiplexDBProvider returns multiple databases using the DBBackend and DBDir
// specified in the Config and uses two levels of subfolders for user and purpose.
func MultiplexDBProvider(ctx *MultiplexDBContext) (multiplex MultiplexDB, err error) {
	dbType := dbm.BackendType(ctx.Config.DBBackend)

	// Storage is located in scopes subfolders per each user
	for _, userAddress := range ctx.Config.GetAddresses() {
		// Uses one subfolder by user
		dbStorage := fmt.Sprintf("%s/%s", ctx.Config.DBDir(), userAddress)

		for _, scope := range ctx.Config.UserScopes[userAddress] {
			// .. and one subfolder by scope
			dbStorage += fmt.Sprintf("/%s", scope)

			userDb, err := dbm.NewDB(ctx.ID, dbType, dbStorage)
			if err != nil {
				return []UserScopedDB{}, err
			}

			scopeID := NewScopeID(userAddress, scope)
			usDB := UserScopedDB{
				UserAddress: userAddress,
				Scope:       scope,
				ScopeHash:   scopeID.Hash(),
				DB:          userDb,
			}

			multiplex = append(multiplex, usDB)
		}
	}

	return multiplex, nil
}

// GetUserScopedDB tries to find a DB in the multiplex using its scope hash
func GetUserScopedDB(
	multiplex MultiplexDB,
	userScopeHash string,
) (usDB UserScopedDB, err error) {
	scopeHash := []byte(userScopeHash)
	if len(scopeHash) != sha256.Size {
		return UserScopedDB{}, fmt.Errorf("incorrect scope hash for user scoped DB, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if idx := slices.IndexFunc(multiplex, func(db UserScopedDB) bool {
		return db.ScopeHash == userScopeHash
	}); idx > -1 {
		return multiplex[idx], nil
	}

	return UserScopedDB{}, fmt.Errorf("could not find user scoped DB using scope hash %s", scopeHash)
}
