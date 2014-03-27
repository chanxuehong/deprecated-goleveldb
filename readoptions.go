package goleveldb

// #cgo LDFLAGS: -lleveldb
// #include "leveldb/c.h"
import "C"

// Options that control read operations
//
// To prevent memory leaks, Destroy must called on a ReadOptions when the
// program no longer needs it.
type ReadOptions struct {
	opt *C.leveldb_readoptions_t
}

// NewReadOptions allocates a new ReadOptions object.
func NewReadOptions() *ReadOptions {
	return &ReadOptions{C.leveldb_readoptions_create()}
}

// Destroy deallocates the ReadOptions, freeing its underlying C struct.
func (ro *ReadOptions) Destroy() {
	C.leveldb_readoptions_destroy(ro.opt)
	ro.opt = nil
}

// If true, all data read from underlying storage will be
// verified against corresponding checksums.
//
//  Default: false
func (ro *ReadOptions) SetVerifyChecksums(b bool) {
	C.leveldb_readoptions_set_verify_checksums(ro.opt, bool2uchar(b))
}

// Should the data read for this iteration be cached in memory?
// Callers may wish to set this field to false for bulk scans.
//
//  Default: true
func (ro *ReadOptions) SetFillCache(b bool) {
	C.leveldb_readoptions_set_fill_cache(ro.opt, bool2uchar(b))
}

// If "snapshot" is non-nil, read as of the supplied snapshot
// (which must belong to the DB that is being read and which must
// not have been released).  If "snapshot" is nil, use an impliicit
// snapshot of the state at the beginning of this read operation.
//
//  Default: nil
func (ro *ReadOptions) SetSnapshot(snap *Snapshot) {
	if snap == nil {
		C.leveldb_readoptions_set_snapshot(ro.opt, nil)
	} else {
		C.leveldb_readoptions_set_snapshot(ro.opt, snap.snap)
	}
}
