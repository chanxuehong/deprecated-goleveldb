package goleveldb

// #cgo LDFLAGS: -lleveldb
// #include "leveldb/c.h"
import "C"

// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in leveldb and are consulted
// automatically by leveldb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB.Get() call.
//
// Most people will want to use the builtin bloom filter support (see
// NewBloomFilterPolicy() below).
//
// To prevent memory leaks, a FilterPolicy must have Destroy called on it when
// it is no longer needed by the program.
type FilterPolicy struct {
	fp *C.leveldb_filterpolicy_t
}

// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key.  A good value for bitsPerKey
// is 10, which yields a filter with ~ 1% false positive rate.
//
// Callers must delete the result after any database that is using the
// result has been closed.
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
func NewBloomFilterPolicy(bitsPerKey int) *FilterPolicy {
	return &FilterPolicy{C.leveldb_filterpolicy_create_bloom(C.int(bitsPerKey))}
}

// Destroy releases the underlying memory of a FilterPolicy.
func (fp *FilterPolicy) Destroy() {
	C.leveldb_filterpolicy_destroy(fp.fp)
	fp.fp = nil
}
