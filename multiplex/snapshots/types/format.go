package types

// ----------------------------------------------------------------------------
// Snapshot Formats

// CurrentNetworkFormat is the currently used format for snapshots. Snapshots
// using the same format must be identical across connected nodes for a given
// height, so this must be bumped when the binary snapshot output changes.
const CurrentNetworkFormat uint32 = 1

// CurrentHistoryFormat is the currently used format for snapshots. Snapshots
// using the same format must be identical across connected nodes for a given
// height, so this must be bumped when the binary snapshot output changes.
const CurrentHistoryFormat uint32 = 2
