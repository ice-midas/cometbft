package types

// ----------------------------------------------------------------------------
// Snapshot Formats

// CurrentFormat is the currently used format for snapshots. Snapshots
// using the same format must be identical across connected nodes for a given
// height, so this must be bumped when the binary snapshot output changes.
const CurrentFormat uint32 = 1
