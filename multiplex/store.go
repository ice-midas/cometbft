package multiplex

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"time"

	"github.com/cosmos/gogoproto/proto"

	cmtstate "github.com/cometbft/cometbft/api/cometbft/state/v1"
	cmtos "github.com/cometbft/cometbft/internal/os"
	sm "github.com/cometbft/cometbft/state"
	store "github.com/cometbft/cometbft/store"
)

// database keys.
var (
	stateKey = []byte("stateKey")
)

type UserScopedStateStore struct {
	UserAddress string
	Scope       string
	ScopeHash   string
	sm.DBStore
}

type UserScopedBlockStore struct {
	UserAddress string
	Scope       string
	ScopeHash   string
	*store.BlockStore
}

type MultiplexStateStore []UserScopedStateStore
type MultiplexBlockStore []*UserScopedBlockStore

// LoadState loads the State from the database.
func (store UserScopedStateStore) Load() (sm.State, error) {
	return store.loadState(stateKey)
}

func (store UserScopedStateStore) loadState(key []byte) (state sm.State, err error) {
	start := time.Now()
	buf, err := store.GetDatabase().Get(key)
	if err != nil {
		return state, err
	}

	addTimeSample(store.StoreOptions.Metrics.StoreAccessDurationSeconds.With("method", "load"), start)()

	if len(buf) == 0 {
		return state, nil
	}

	sp := new(cmtstate.State)

	err = proto.Unmarshal(buf, sp)
	if err != nil {
		// DATA HAS BEEN CORRUPTED OR THE SPEC HAS CHANGED
		cmtos.Exit(fmt.Sprintf(`LoadState: Data has been corrupted or its spec has changed:
		%v\n`, err))
	}

	sm, err := sm.FromProto(sp)
	if err != nil {
		return state, err
	}
	return *sm, nil
}

// func (bs UserScopedBlockStore) GetStorePtr() *store.BlockStore {
// 	return bs.BlockStore
// }

// ----------------------------------------------------------------------------
// Factories

// NewMultiplexStateStore creates multiple dbStores of the state pkg.
func NewMultiplexStateStore(multiplex MultiplexDB, options sm.StoreOptions) (mStore MultiplexStateStore) {
	for _, db := range multiplex {
		mStore = append(mStore, UserScopedStateStore{
			UserAddress: db.UserAddress,
			Scope:       db.Scope,
			ScopeHash:   db.ScopeHash,
			DBStore:     sm.NewStore(db, options).(sm.DBStore),
		})
	}

	return mStore
}

// NewMultiplexBlockStore returns multiple BlockStores from the given multiplex DB,
// initialized to the last height that was committed to the corresponding DB.
func NewMultiplexBlockStore(
	multiplex MultiplexDB,
	options ...store.BlockStoreOption,
) (mbStore MultiplexBlockStore) {
	//XXX multiplex block store should use *BlockStore
	for _, db := range multiplex {
		bs := store.NewBlockStore(db, options...)
		mbStore = append(mbStore, &UserScopedBlockStore{
			UserAddress: db.UserAddress,
			Scope:       db.Scope,
			ScopeHash:   db.ScopeHash,
			BlockStore:  bs,
		})
	}

	return mbStore
}

// GetUserScopedState tries to find a state store in the multiplex using its scope hash
func GetUserScopedStateStore(
	multiplex MultiplexStateStore,
	userScopeHash string,
) (UserScopedStateStore, error) {
	scopeHash := []byte(userScopeHash)
	if len(scopeHash) != sha256.Size {
		return UserScopedStateStore{}, fmt.Errorf("incorrect scope hash for state store multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if idx := slices.IndexFunc(multiplex, func(dbs UserScopedStateStore) bool {
		return dbs.ScopeHash == userScopeHash
	}); idx > -1 {
		return multiplex[idx], nil
	}

	return UserScopedStateStore{}, fmt.Errorf("could not find state store in multiplex using scope hash %s", scopeHash)
}

// GetUserScopedBlock tries to find a block store in the multiplex using its scope hash
func GetUserScopedBlockStore(
	multiplex MultiplexBlockStore,
	userScopeHash string,
) (*UserScopedBlockStore, error) {
	scopeHash := []byte(userScopeHash)
	if len(scopeHash) != sha256.Size {
		return nil, fmt.Errorf("incorrect scope hash for block store multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if idx := slices.IndexFunc(multiplex, func(dbs *UserScopedBlockStore) bool {
		return dbs.ScopeHash == userScopeHash
	}); idx > -1 {
		return multiplex[idx], nil
	}

	return nil, fmt.Errorf("could not find block store in multiplex using scope hash %s", scopeHash)
}
