package goleveldb

// #cgo LDFLAGS: -lleveldb
// #include "leveldb/c.h"
import "C"

import (
	"unsafe"
)

// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//  batch.Put("key", "v1");
//  batch.Delete("key");
//  batch.Put("key", "v2");
//  batch.Put("key", "v3");
//
// Multiple goroutines can invoke const methods on a WriteBatch without
// external synchronization, but if any of the goroutines may call a
// non-const method, all goroutines accessing the same WriteBatch must use
// external synchronization.
//
// To prevent memory leaks, call Destroy when the program no longer needs the
// WriteBatch object.
type WriteBatch struct {
	wbatch *C.leveldb_writebatch_t
}

// NewWriteBatch creates a fully allocated WriteBatch.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{C.leveldb_writebatch_create()}
}

// Destroy releases the underlying memory of a WriteBatch.
func (w *WriteBatch) Destroy() {
	C.leveldb_writebatch_destroy(w.wbatch)
	w.wbatch = nil
}

// Store the mapping "key->value" in the database.
//
// Both the key and value byte slices may be reused as WriteBatch takes a copy
// of them before returning.
func (w *WriteBatch) Put(key, value []byte) {
	var keyPtr, valuePtr *C.char
	var keyLen, valueLen = len(key), len(value)

	if keyLen == 0 {
		keyPtr = (*C.char)(unsafe.Pointer(emptyKeyPtr))
	} else {
		keyPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}

	if valueLen == 0 {
		valuePtr = (*C.char)(unsafe.Pointer(emptyValuePtr))
	} else {
		valuePtr = (*C.char)(unsafe.Pointer(&value[0]))
	}

	// leveldb_writebatch_put, and _delete call memcpy() (by way of
	// Memtable::Add) when called, so we do not need to worry about these
	// []byte being reclaimed by GC.
	C.leveldb_writebatch_put(w.wbatch,
		keyPtr, C.size_t(keyLen),
		valuePtr, C.size_t(valueLen))
}

// If the database contains a mapping for "key", erase it.
// Else do nothing.
//
// The key byte slice may be reused safely. Delete takes a copy of
// them before returning.
func (w *WriteBatch) Delete(key []byte) {
	var keyPtr *C.char
	var keyLen = len(key)

	if keyLen == 0 {
		keyPtr = (*C.char)(unsafe.Pointer(emptyKeyPtr))
	} else {
		keyPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}
	// leveldb_writebatch_put, and _delete call memcpy() (by way of
	// Memtable::Add) when called, so we do not need to worry about these
	// []byte being reclaimed by GC.
	C.leveldb_writebatch_delete(w.wbatch, keyPtr, C.size_t(keyLen))
}

// Clear all updates buffered in this batch.
func (w *WriteBatch) Clear() {
	C.leveldb_writebatch_clear(w.wbatch)
}
