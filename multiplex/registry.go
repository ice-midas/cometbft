package multiplex

import (
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
)

// NodeRegistry contains private node configuration such as the PrivValidator
// and a Config instance, and a multiplex of bootstrapped node instances.
type NodeRegistry struct {
	config   *cfg.Config
	nodeInfo p2p.NodeInfo
	nodeKey  *p2p.NodeKey // our node privkey

	Nodes   MultiplexNode
	Scopes  []string
	Reactor *Reactor
}

// GetListenAddresses returns a service address map by user scope hash.
// Supports listen address rewrites for: p2p, rpc, grpc and grpc privileged.
func (r *NodeRegistry) GetListenAddresses() MultiplexServiceAddress {
	laddrs := make(MultiplexServiceAddress, len(r.Nodes))
	for scopeHash, node := range r.Nodes {
		laddrs[scopeHash] = map[string]string{
			"P2P":      node.Config().P2P.ListenAddress,
			"RPC":      node.Config().RPC.ListenAddress,
			"GRPC":     node.Config().GRPC.ListenAddress,
			"GRPCPriv": node.Config().GRPC.Privileged.ListenAddress,
		}
	}

	return laddrs
}
