package multiplex

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"

	mxp2p "github.com/cometbft/cometbft/api/cometbft/multiplex/v1"
	tmp2p "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	cmtstrings "github.com/cometbft/cometbft/internal/strings"
	cmtbytes "github.com/cometbft/cometbft/libs/bytes"
	"github.com/cometbft/cometbft/p2p"
)

// HistoricalNodeInfo is the node information exchanged by historical nodes
// of a multi-network infra during the CometBFT P2P handshake.
type HistoricalNodeInfo struct {
	// Replication configuration
	Networks         []string               `json:"networks"` // contains ChainIDs
	ProtocolVersions []ChainProtocolVersion `json:"protocol_versions"`

	// Authenticate
	// TODO: replace with NetAddress
	DefaultNodeID p2p.ID `json:"id"`          // authenticated identifier
	ListenAddr    string `json:"listen_addr"` // default accepting incoming (first network)

	// Check compatibility.
	// Channels are HexBytes so easier to read as JSON
	Version  string            `json:"version"`  // major.minor.revision
	Channels cmtbytes.HexBytes `json:"channels"` // channels this node knows about

	// ASCIIText fields
	Moniker string                   `json:"moniker"` // arbitrary moniker
	Other   p2p.DefaultNodeInfoOther `json:"other"`   // other application specific data
}

// Assert HistoricalNodeInfo satisfies NodeInfo.
var _ p2p.NodeInfo = HistoricalNodeInfo{}

// ID returns the node's peer ID.
func (info HistoricalNodeInfo) ID() p2p.ID {
	return info.DefaultNodeID
}

// GetChannels returns the node's channels.
func (info HistoricalNodeInfo) GetChannels() cmtbytes.HexBytes {
	return info.Channels
}

// Validate checks the self-reported HistoricalNodeInfo is safe.
// It returns an error if there are too many Channels or if there are
// any duplicate Channels.
func (info HistoricalNodeInfo) Validate() error {
	// ID is already validated.

	// Validate P2P listen address
	if _, err := info.NetAddress(); err != nil {
		return err
	}

	// Validate Version
	if len(info.Version) > 0 &&
		(!cmtstrings.IsASCIIText(info.Version) || cmtstrings.ASCIITrim(info.Version) == "") {
		return fmt.Errorf("info.Version must be valid ASCII text without tabs, but got %v", info.Version)
	}

	// Validate Channels - ensure max and check for duplicates.
	if len(info.Channels) > p2p.MaxNumChannels() {
		return fmt.Errorf("info.Channels is too long (%v). Max is %v", len(info.Channels), p2p.MaxNumChannels())
	}
	channels := make(map[byte]struct{})
	for _, ch := range info.Channels {
		if _, ok := channels[ch]; ok {
			return fmt.Errorf("info.Channels contains duplicate channel id %v", ch)
		}
		channels[ch] = struct{}{}
	}

	// Validate Moniker.
	if !cmtstrings.IsASCIIText(info.Moniker) || cmtstrings.ASCIITrim(info.Moniker) == "" {
		return fmt.Errorf("info.Moniker must be valid non-empty ASCII text without tabs, but got %v", info.Moniker)
	}

	// Validate Other.
	other := info.Other
	txIndex := other.TxIndex
	switch txIndex {
	case "", "on", "off":
	default:
		return fmt.Errorf("info.Other.TxIndex should be either 'on', 'off', or empty string, got '%v'", txIndex)
	}

	// Validate RPC address
	rpcAddr := info.Other.RPCAddress
	if len(rpcAddr) > 0 && (!cmtstrings.IsASCIIText(rpcAddr) || cmtstrings.ASCIITrim(rpcAddr) == "") {
		return fmt.Errorf("info.Other.RPCAddress=%v must be valid ASCII text without tabs", rpcAddr)
	}

	return nil
}

// CompatibleWith checks if two [p2p.NodeInfo] are compatible with each other.
//
// This implementation of CompatibleWith verifies that at least one of the
// replicated chains is compatible with the other peer's replicated chains.
//
// CONTRACT: two nodes are compatible if the Block version and network match
// and they have at least one channel in common.
func (info HistoricalNodeInfo) CompatibleWith(otherInfo p2p.NodeInfo) error {
	// We can only communicate with other multi-network nodes.
	other, ok := otherInfo.(MultiNetworkNodeInfo)
	if !ok {
		return fmt.Errorf(
			"wrong NodeInfo type. Expected MultiNetworkNodeInfo, got %v", reflect.TypeOf(otherInfo))
	}

	haveCommonReplicatedChain := false
	for _, otherProtocolVersion := range other.ProtocolVersions {
		otherChainID := otherProtocolVersion.ChainID
		versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ChainProtocolVersion) bool {
			return v.ChainID == otherChainID
		})

		// Not having *all* the same replicated chains is allowed
		if versionPos < 0 {
			continue
		}

		localProtocolVersion := info.ProtocolVersions[versionPos]

		// Block versions must be the same on both nodes
		if localProtocolVersion.Block != otherProtocolVersion.Block {
			// nodes must share a block version
			return fmt.Errorf("peer is on a different Block version for ChainID %s. Got %v, expected %v",
				localProtocolVersion.ChainID, otherProtocolVersion.Block, localProtocolVersion.Block)
		}

		// Make sure we also have the network information such as ChainID
		networkPos := slices.IndexFunc(info.Networks, func(v string) bool {
			return v == otherChainID
		})

		// Not having *all* the same replicated chains is allowed
		if networkPos < 0 {
			continue
		}

		haveCommonReplicatedChain = true
	}

	// nodes must share at least one replicated chain
	if !haveCommonReplicatedChain {
		return errors.New("peer does not have at least one replicated chain in common")
	}

	// if we have no channels, we're just testing
	if len(info.Channels) == 0 {
		return nil
	}

	// for each of our channels, check if they have it
	found := false
OUTER_LOOP:
	for _, ch1 := range info.Channels {
		for _, ch2 := range other.Channels {
			if ch1 == ch2 {
				found = true
				break OUTER_LOOP // only need one
			}
		}
	}
	if !found {
		return fmt.Errorf("peer has no common channels. Our channels: %v ; Peer channels: %v", info.Channels, other.Channels)
	}
	return nil
}

// NetAddress returns a NetAddress derived from the HistoricalNodeInfo -
// it includes the authenticated peer ID and the self-reported
// ListenAddr. Note that the ListenAddr is not authenticated and
// may not match that address actually dialed if its an outbound peer.
func (info HistoricalNodeInfo) NetAddress() (*p2p.NetAddress, error) {
	idAddr := p2p.IDAddressString(info.ID(), info.ListenAddr)
	return p2p.NewNetAddressString(idAddr)
}

func (info HistoricalNodeInfo) HasChannel(chID byte) bool {
	return bytes.Contains(info.Channels, []byte{chID})
}

func (info HistoricalNodeInfo) ToProto() *mxp2p.HistoricalNodeInfo {

	numReplicatedChains := len(info.Networks)
	numVersions := len(info.ProtocolVersions)

	// Mismatch in sizes should never happen here
	if numReplicatedChains != numVersions {
		panic(fmt.Sprintf("found inconsistent number of replicated chains, got %d networks and %d versions",
			numReplicatedChains, numVersions))
	}

	dni := new(mxp2p.HistoricalNodeInfo)
	dni.Networks = make([]string, numReplicatedChains)
	dni.ProtocolVersions = make([]*mxp2p.ChainProtocolVersion, numReplicatedChains)

	for i, userChainID := range info.Networks {

		versionPos := slices.IndexFunc(info.ProtocolVersions, func(v ChainProtocolVersion) bool {
			return v.ChainID == userChainID
		})

		networkPos := slices.IndexFunc(info.Networks, func(n string) bool {
			return n == userChainID
		})

		// Not being able to find a protocol version or network should never happen
		if versionPos < 0 || networkPos < 0 {
			panic(fmt.Sprintf("could not determine version and listen address for ChainID %s", userChainID))
		}

		protocolVersion := info.ProtocolVersions[versionPos]

		dni.Networks[i] = userChainID
		dni.ProtocolVersions[i] = &mxp2p.ChainProtocolVersion{
			ChainID: userChainID,
			ProtocolVersion: &tmp2p.ProtocolVersion{
				P2P:   protocolVersion.P2P,
				Block: protocolVersion.Block,
				App:   protocolVersion.App,
			},
		}
	}

	dni.DefaultNodeID = string(info.DefaultNodeID)
	dni.ListenAddr = info.ListenAddr
	dni.Version = info.Version
	dni.Channels = info.Channels
	dni.Moniker = info.Moniker
	dni.Other = tmp2p.DefaultNodeInfoOther{
		TxIndex:    info.Other.TxIndex,
		RPCAddress: info.Other.RPCAddress,
	}

	return dni
}

func HistoricalNodeInfoFromProto(pb *mxp2p.HistoricalNodeInfo) (HistoricalNodeInfo, error) {
	if pb == nil {
		return HistoricalNodeInfo{}, errors.New("nil node info")
	}

	networks := make([]string, len(pb.Networks))
	protocolVersions := make([]ChainProtocolVersion, len(pb.ProtocolVersions))

	for i, chainId := range pb.Networks {
		networks[i] = chainId
	}

	for i, pv := range pb.ProtocolVersions {
		protocolVersions[i] = ChainProtocolVersion{
			ChainID: pv.ChainID,
			P2P:     pv.ProtocolVersion.P2P,
			Block:   pv.ProtocolVersion.Block,
			App:     pv.ProtocolVersion.App,
		}
	}

	dni := HistoricalNodeInfo{
		Networks:         networks,
		ProtocolVersions: protocolVersions,
		DefaultNodeID:    p2p.ID(pb.DefaultNodeID),
		ListenAddr:       pb.ListenAddr,
		Version:          pb.Version,
		Channels:         pb.Channels,
		Moniker:          pb.Moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    pb.Other.TxIndex,
			RPCAddress: pb.Other.RPCAddress,
		},
	}

	return dni, nil
}
