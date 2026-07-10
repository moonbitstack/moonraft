// Transitional local aliases: while the consensus core still lives in the root
// package, these bring the already-extracted sub-package types into unqualified
// scope so the un-moved sources compile unchanged. Removed once every source
// file has migrated to its sub-package and the root becomes a pure facade.

///|
using @tracker {type Progress, type ProgressState}
