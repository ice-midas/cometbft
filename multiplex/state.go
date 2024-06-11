package multiplex

import (
	"crypto/sha256"
	"fmt"
	"slices"

	sm "github.com/cometbft/cometbft/state"
)

type ScopedState struct {
	ScopeHash string
	sm.State
}

type MultiplexState []ScopedState

func (s ScopedState) GetState() sm.State {
	return s.Copy()
}

// GetScopedState tries to find a state in the multiplex using its scope hash
func GetScopedState(
	multiplex MultiplexState,
	userScopeHash string,
) (usDB ScopedState, err error) {
	scopeHash := []byte(userScopeHash)
	if len(scopeHash) != sha256.Size {
		return ScopedState{}, fmt.Errorf("incorrect scope hash for state multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if idx := slices.IndexFunc(multiplex, func(s ScopedState) bool {
		return s.ScopeHash == userScopeHash
	}); idx > -1 {
		return multiplex[idx], nil
	}

	return ScopedState{}, fmt.Errorf("could not find state in multiplex using scope hash %s", scopeHash)
}
