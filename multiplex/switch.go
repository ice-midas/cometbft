package multiplex

import (
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
)

type ScopedSwitch struct {
	ScopeHash string
	*p2p.Switch
}

// XXX multiplex objects should be: type xMultiplex map[string]*x
type SwitchMultiplex []*ScopedSwitch

// NewSwitch creates a new Switch with the given config.
func NewScopedSwitch(
	cfg *config.P2PConfig,
	transport p2p.Transport,
	userScopeHash string,
	options ...p2p.SwitchOption,
) *ScopedSwitch {
	sw := p2p.NewSwitch(cfg, transport, options...)
	ssw := &ScopedSwitch{
		ScopeHash: userScopeHash,
		Switch:    sw,
	}

	return ssw
}
