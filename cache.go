package goleveldb

// #cgo LDFLAGS: -lleveldb
// #include "leveldb/c.h"
import "C"

// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)
//
// To prevent memory leaks, a Cache must have Destroy called on it when it is
// no longer needed by the program.
//  NOTE: if the process is shutting down,
//  this may not be necessary and could be avoided to shorten shutdown time.
type Cache struct {
	cache *C.leveldb_cache_t
}

// NewLRUCache create a new cache with a fixed size capacity.
// This implementation of Cache uses a least-recently-used eviction policy.
//
// To prevent memory leaks, Destroy should be called on the Cache when the
// program no longer needs it.
func NewLRUCache(capacity int) *Cache {
	return &Cache{C.leveldb_cache_create_lru(C.size_t(capacity))}
}

// Destroy deallocates the underlying memory of the Cache object.
func (c *Cache) Destroy() {
	C.leveldb_cache_destroy(c.cache)
	c.cache = nil
}
