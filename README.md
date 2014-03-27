# goleveldb

goleveldb is a Go wrapper for LevelDB.

## Building

You'll need the shared library build of
[LevelDB](http://code.google.com/p/leveldb/) installed on your machine. The
current LevelDB will build it by default.

The minimum version of LevelDB required is currently 1.6. If you require the
use of an older version of LevelDB, see the [fork of levigo for LevelDB
1.4](https://github.com/jmhodges/levigo_leveldb_1.4). Prefer putting in the
work to be up to date as LevelDB moves very quickly.

Now, if you build LevelDB and put the shared library and headers in one of the
standard places for your OS, you'll be able to simply run:

    go get github.com/chanxuehong/goleveldb

But, suppose you put the shared LevelDB library somewhere weird like
/path/to/lib and the headers were installed in /path/to/include. To install
goleveldb remotely, you'll run:

    CGO_CFLAGS="-I/path/to/leveldb/include" CGO_LDFLAGS="-L/path/to/leveldb/lib" go get github.com/chanxuehong/goleveldb

and there you go.

In order to build with snappy, you'll have to explicitly add "-lsnappy" to the
`CGO_LDFLAGS`. Supposing that both snappy and leveldb are in weird places,
you'll run something like:

    CGO_CFLAGS="-I/path/to/leveldb/include -I/path/to/snappy/include"
    CGO_LDFLAGS="-L/path/to/leveldb/lib -L/path/to/snappy/lib -lsnappy" go get github.com/chanxuehong/goleveldb

(and make sure the -lsnappy is after the snappy library path!).

Of course, these same rules apply when doing `go build`, as well.

## Caveats

Comparators and WriteBatch iterators must be written in C in your own
library. This seems like a pain in the ass, but remember that you'll have the
LevelDB C API available to your in your client package when you import goleveldb.
