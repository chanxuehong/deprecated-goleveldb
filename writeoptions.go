package goleveldb

// #cgo LDFLAGS: -lleveldb
// #include "leveldb/c.h"
import "C"

// Options that control write operations
//
// To prevent memory leaks, Destroy must called on a WriteOptions when the
// program no longer needs it.
type WriteOptions struct {
	opt *C.leveldb_writeoptions_t
}

// NewWriteOptions allocates a new WriteOptions object.
func NewWriteOptions() *WriteOptions {
	return &WriteOptions{C.leveldb_writeoptions_create()}
}

// Destroy deallocates the WriteOptions, freeing its underlying C struct.
func (wo *WriteOptions) Destroy() {
	C.leveldb_writeoptions_destroy(wo.opt)
	wo.opt = nil
}

// If true, the write will be flushed from the operating system
// buffer cache (by calling WritableFile::Sync()) before the write
// is considered complete.  If this flag is true, writes will be
// slower.
//
// If this flag is false, and the machine crashes, some recent
// writes may be lost.  Note that if it is just the process that
// crashes (i.e., the machine does not reboot), no writes will be
// lost even if sync==false.
//
// In other words, a DB write with sync==false has similar
// crash semantics as the "write()" system call.  A DB write
// with sync==true has similar crash semantics to a "write()"
// system call followed by "fsync()".
//
//  Default: false
func (wo *WriteOptions) SetSync(b bool) {
	C.leveldb_writeoptions_set_sync(wo.opt, bool2uchar(b))
}
