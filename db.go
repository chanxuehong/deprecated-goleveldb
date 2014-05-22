package goleveldb

/*
#cgo LDFLAGS: -lleveldb
#include <stdlib.h>
#include "leveldb/c.h"

// This function exists only to clean up lack-of-const warnings when
// leveldb_approximate_sizes is called from Go-land.
void goleveldb_leveldb_approximate_sizes(
	leveldb_t* db,
	int num_ranges,
	char** range_start_key, const size_t* range_start_key_len,
	char** range_limit_key, const size_t* range_limit_key_len,
	uint64_t* sizes) {

	leveldb_approximate_sizes(
		db,
		num_ranges,
		(const char* const*)range_start_key, range_start_key_len,
		(const char* const*)range_limit_key, range_limit_key_len,
		sizes);
}
*/
import "C"

import (
	"errors"
	"strconv"
	"unsafe"
)

// ErrNotFound means that a get call did not find the requested key.
var ErrNotFound = errors.New("goleveldb: not found")

// Range is a range of keys in the database.
type Range struct {
	Start []byte // Included in the range
	Limit []byte // Not included in the range
}

// Snapshot provides a consistent view of read operations in a DB. It is set
// on to a ReadOptions and passed in. It is only created by DB.NewSnapshot.
//
// To prevent memory leaks and resource strain in the database, the snapshot
// returned must be released with DB.ReleaseSnapshot method on the DB that
// created it.
type Snapshot struct {
	snap *C.leveldb_snapshot_t
}

// A DB is a persistent ordered map from keys to values.
// A DB is safe for concurrent access from multiple goroutines without
// any external synchronization.
//
// To avoid memory and file descriptor leaks, call Close when the process no
// longer needs the handle. Calls to any DB method made after Close will
// panic.
type DB struct {
	db          *C.leveldb_t
	defaultROpt *ReadOptions
	defaultWOpt *WriteOptions
}

// Open is shorthand for OpenEx(dbname, opt, nil, nil).
func Open(dbname string, opt *Options) (*DB, error) {
	return OpenEx(dbname, opt, nil, nil)
}

// OpenEx open the database with the specified "dbname".
// Returned a pointer to a heap-allocated database and nil error.
// Returned a nil pointer and an error.
//
// DB.Close() should called when it is no longer needed.
//
// Set the Options opt default if nil
// Set the ReadOptions defaultROpt default if nil
// Set the WriteOptions defaultWOpt default if nil
func OpenEx(dbname string, opt *Options,
	defaultROpt *ReadOptions, defaultWOpt *WriteOptions) (*DB, error) {

	if opt == nil {
		opt = NewOptions()
		defer opt.Destroy()
	}

	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	var errStr *C.char
	leveldb := C.leveldb_open(opt.opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return nil, errors.New(gs)
	}

	if defaultROpt == nil {
		defaultROpt = NewReadOptions()
	}
	if defaultWOpt == nil {
		defaultWOpt = NewWriteOptions()
	}
	return &DB{
		db:          leveldb,
		defaultROpt: defaultROpt,
		defaultWOpt: defaultWOpt}, nil
}

// Destroy the contents of the specified database.
// Be very careful using this method.
//
// Set the Options default if o == nil
func DestroyDatabase(dbname string, o *Options) error {
	if o == nil {
		o = NewOptions()
		defer o.Destroy()
	}

	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	var errStr *C.char
	C.leveldb_destroy_db(o.opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return errors.New(gs)
	}
	return nil
}

// If a DB cannot be opened, you may attempt to call this method to
// resurrect as much of the contents of the database as possible.
// Some data may be lost, so be careful when calling this function
// on a database that contains important information.
//
// Set the Options default if o == nil
func RepairDatabase(dbname string, o *Options) error {
	if o == nil {
		o = NewOptions()
		defer o.Destroy()
	}

	ldbname := C.CString(dbname)
	defer C.free(unsafe.Pointer(ldbname))

	var errStr *C.char
	C.leveldb_repair_db(o.opt, ldbname, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return errors.New(gs)
	}
	return nil
}

// Close the database, rendering it unusable for I/O, by deallocating
// the underlying handle.
//
// Any attempts to use the DB after Close is called will panic.
func (db *DB) Close() {
	C.leveldb_close(db.db)
	db.db = nil

	db.defaultROpt.Destroy()
	db.defaultROpt = nil

	db.defaultWOpt.Destroy()
	db.defaultWOpt = nil
}

func (db *DB) MajorVersion() int {
	return int(C.leveldb_major_version())
}

func (db *DB) MinorVersion() int {
	return int(C.leveldb_minor_version())
}

// The following variables exists only to get a valid address for
// empty key and value.
// they were used in DB.Put, DB.Get, DB.Delete, WriteBatch.Put, WriteBatch.Delete.
var (
	empty         = 0 // unused anywhere
	emptyKeyPtr   = &empty
	emptyValuePtr = &empty
)

// Set the database entry for "key" to "value".  Returns nil on success,
//  NOTE: consider WriteOptions.SetSync(true).
//
// If a nil []byte is passed in as value, it will be returned by Get as an
// zero-length slice.
//
// The key and value byte slices may be reused safely. Put takes a copy of
// them before returning.
//
// Set the WriteOptions default if wo == nil
func (db *DB) Put(wo *WriteOptions, key, value []byte) error {
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

	if wo == nil {
		wo = db.defaultWOpt
	}

	var errStr *C.char
	// leveldb_put, _get, and _delete call memcpy() (by way of Memtable::Add)
	// when called, so we do not need to worry about these []byte being
	// reclaimed by GC.
	C.leveldb_put(
		db.db,
		wo.opt,
		keyPtr, C.size_t(keyLen),
		valuePtr, C.size_t(valueLen),
		&errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return errors.New(gs)
	}
	return nil
}

// Get returns the data associated with the key from the database.
//
// If the key does not exist in the database, ErrNotFound is returned.
//  NOTE: May return some other errors.
//
// If the key does exist, but the data is zero-length in the database, a zero-length
// []byte will be returned.
//
// The key byte slice may be reused safely. Get takes a copy of
// them before returning.
//
// Set the ReadOptions default if ro == nil
func (db *DB) Get(ro *ReadOptions, key []byte) (value []byte, err error) {
	var keyPtr *C.char
	var keyLen = len(key)

	if keyLen == 0 {
		keyPtr = (*C.char)(unsafe.Pointer(emptyKeyPtr))
	} else {
		keyPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}

	if ro == nil {
		ro = db.defaultROpt
	}

	var errStr *C.char
	var vallen C.size_t
	// leveldb_put, _get, and _delete call memcpy() (by way of Memtable::Add)
	// when called, so we do not need to worry about these []byte being
	// reclaimed by GC.
	cvalue := C.leveldb_get(
		db.db,
		ro.opt,
		keyPtr, C.size_t(keyLen),
		&vallen,
		&errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return nil, errors.New(gs)
	}

	if cvalue == nil {
		return nil, ErrNotFound
	}

	value = C.GoBytes(unsafe.Pointer(cvalue), C.int(vallen))
	C.leveldb_free(unsafe.Pointer(cvalue))
	return
}

// Remove the database entry (if any) for "key".  Returns nil on
// success, and a non-nil on error.  It is not an error if "key"
// did not exist in the database.
//  NOTE: consider WriteOptions.SetSync(true).
//
// The key byte slice may be reused safely. Delete takes a copy of
// them before returning.
//
// Set the WriteOptions default if wo == nil
func (db *DB) Delete(wo *WriteOptions, key []byte) error {
	var keyPtr *C.char
	var keyLen = len(key)

	if keyLen == 0 {
		keyPtr = (*C.char)(unsafe.Pointer(emptyKeyPtr))
	} else {
		keyPtr = (*C.char)(unsafe.Pointer(&key[0]))
	}

	if wo == nil {
		wo = db.defaultWOpt
	}

	var errStr *C.char
	// leveldb_put, _get, and _delete call memcpy() (by way of Memtable::Add)
	// when called, so we do not need to worry about these []byte being
	// reclaimed by GC.
	C.leveldb_delete(
		db.db,
		wo.opt,
		keyPtr, C.size_t(keyLen),
		&errStr)

	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return errors.New(gs)
	}
	return nil
}

// Apply the specified updates to the database.
// Returns nil on success, non-nil on failure.
//  NOTE: consider WriteOptions.SetSync(true).
//
// Set the WriteOptions default if wo == nil
func (db *DB) Write(wo *WriteOptions, wb *WriteBatch) error {
	if wo == nil {
		wo = db.defaultWOpt
	}

	var errStr *C.char
	C.leveldb_write(db.db, wo.opt, wb.wbatch, &errStr)
	if errStr != nil {
		gs := C.GoString(errStr)
		C.leveldb_free(unsafe.Pointer(errStr))
		return errors.New(gs)
	}
	return nil
}

// NewIterator returns an Iterator over the the database that uses the
// ReadOptions given.
//
// Often, this is used for large, offline bulk reads while serving live
// traffic. In that case, it may be wise to disable caching so that the data
// processed by the returned Iterator does not displace the already cached
// data. This can be done by calling SetFillCache(false) on the ReadOptions
// before passing it here.
//
// Similiarly, ReadOptions.SetSnapshot is also useful.
//
// Set the ReadOptions default if ro == nil
func (db *DB) NewIterator(ro *ReadOptions) *Iterator {
	if ro == nil {
		ro = db.defaultROpt
	}

	it := C.leveldb_create_iterator(db.db, ro.opt)
	return &Iterator{iter: it}
}

// GetSnapshot creates a new snapshot of the database.
//
// The snapshot, when used in a ReadOptions, provides a consistent view of
// state of the database at the the snapshot was created.
//
// To prevent memory leaks and resource strain in the database, the snapshot
// returned must be released with DB.ReleaseSnapshot method on the DB that
// created it.
//
// See the LevelDB documentation for details.
func (db *DB) GetSnapshot() *Snapshot {
	snap := C.leveldb_create_snapshot(db.db)
	return &Snapshot{snap: snap}
}

// ReleaseSnapshot removes the snapshot from the database's list of snapshots,
// and deallocates it.
func (db *DB) ReleaseSnapshot(snap *Snapshot) {
	C.leveldb_release_snapshot(db.db, snap.snap)
}

// GetProperty returns the value of a database property.
//
// If "property" is not a valid property understood by this
// DB implementation, return empty string.
//
// Valid property names include:
//
//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
//     where <N> is an ASCII representation of a level number (e.g. "0").
//  "leveldb.stats" - returns a multi-line string that describes statistics
//     about the internal operation of the DB.
//  "leveldb.sstables" - returns a multi-line string that describes all
//     of the sstables that make up the db contents.
func (db *DB) GetProperty(property string) (value string) {
	cname := C.CString(property)
	cvalue := C.leveldb_property_value(db.db, cname)
	C.free(unsafe.Pointer(cname))
	if cvalue == nil {
		return ""
	} else {
		value = C.GoString(cvalue)
		C.leveldb_free(unsafe.Pointer(cvalue))
		return
	}
}

// For each i in [ 0..len(ranges) ), store in "sizes[i]", the approximate
// file system space used by keys in "[ranges[i].Start .. ranges[i].Limit)".
//
// Note that the returned sizes measure file system space usage, so
// if the user data compresses by a factor of ten, the returned
// sizes will be one-tenth the size of the corresponding user data size.
//
// The results may not include the sizes of recently written data.
func (db *DB) GetApproximateSizes(ranges []Range) (sizes []uint64) {
	rangeNum := len(ranges)
	if rangeNum == 0 {
		return make([]uint64, 0)
	}

	num_ranges := C.int(rangeNum)
	range_start_key := make([]*C.char, rangeNum)
	range_start_key_len := make([]C.size_t, rangeNum)
	range_limit_key := make([]*C.char, rangeNum)
	range_limit_key_len := make([]C.size_t, rangeNum)
	csizes := make([]C.uint64_t, rangeNum)

	for i, rang := range ranges {
		if len(rang.Start) == 0 {
			range_start_key[i] = (*C.char)(unsafe.Pointer(emptyKeyPtr))
		} else {
			range_start_key[i] = (*C.char)(unsafe.Pointer(&rang.Start[0]))
		}
		range_start_key_len[i] = C.size_t(len(rang.Start))

		if len(rang.Limit) == 0 {
			range_limit_key[i] = (*C.char)(unsafe.Pointer(emptyKeyPtr))
		} else {
			range_limit_key[i] = (*C.char)(unsafe.Pointer(&rang.Limit[0]))
		}
		range_limit_key_len[i] = C.size_t(len(rang.Limit))
	}

	C.goleveldb_leveldb_approximate_sizes(
		db.db,
		num_ranges,
		&range_start_key[0], &range_start_key_len[0],
		&range_limit_key[0], &range_limit_key_len[0],
		&csizes[0])

	sizes = make([]uint64, rangeNum)
	for i := 0; i < rangeNum; i++ {
		sizes[i] = uint64(csizes[i])
	}
	return
}

// Compact the underlying storage for the key range [begin, end].
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data.  This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// begin==nil is treated as a key before all keys in the database.
// end==nil is treated as a key after all keys in the database.
// Therefore the following call will compact the entire database:
//
//  db.CompactRange(nil, nil);
func (db *DB) CompactRange(begin, end []byte) {
	var beginPtr, endPtr *C.char
	var beginLen, endLen = len(begin), len(end)
	if beginLen != 0 {
		beginPtr = (*C.char)(unsafe.Pointer(&begin[0]))
	}
	if endLen != 0 {
		endPtr = (*C.char)(unsafe.Pointer(&end[0]))
	}

	C.leveldb_compact_range(
		db.db,
		beginPtr, C.size_t(beginLen),
		endPtr, C.size_t(endLen))
}
