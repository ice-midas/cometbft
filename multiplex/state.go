package multiplex

import (
	"crypto/sha256"
	"fmt"
	"slices"

	sm "github.com/cometbft/cometbft/state"
)

type UserScopedState struct {
	UserAddress string
	Scope       string
	ScopeHash   string
	sm.State
}

type MultiplexState []UserScopedState

func (s UserScopedState) GetState() sm.State {
	return s.Copy()
}

// GetUserScopedState tries to find a state in the multiplex using its scope hash
func GetUserScopedState(
	multiplex MultiplexState,
	userScopeHash string,
) (usDB UserScopedState, err error) {
	scopeHash := []byte(userScopeHash)
	if len(scopeHash) != sha256.Size {
		return UserScopedState{}, fmt.Errorf("incorrect scope hash for state multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if idx := slices.IndexFunc(multiplex, func(s UserScopedState) bool {
		return s.ScopeHash == userScopeHash
	}); idx > -1 {
		return multiplex[idx], nil
	}

	return UserScopedState{}, fmt.Errorf("could not find state in multiplex using scope hash %s", scopeHash)
}
