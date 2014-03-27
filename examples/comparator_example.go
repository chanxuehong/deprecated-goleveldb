package main

/*
#cgo LDFLAGS: -lleveldb
#include <string.h>
#include <leveldb/c.h>

static void MyComparatorDestructor(void* state) {}

// A simple implementation of compare
static int MyComparatorCompare(void* state,
	const char* a, size_t alen,
	const char* b, size_t blen) {

	int n = (alen < blen) ? alen : blen;
	int r = memcmp(a, b, n);

	if (r == 0) {
		if (alen < blen) {
			r = -1;
		} else if (alen > blen) {
			r = +1;
		}
	}
	return r;
}

static const char* MyComparatorName(void* state) {
	return "MyComparator";
}

static leveldb_comparator_t* NewMyComparator() {
	return leveldb_comparator_create(NULL,
		MyComparatorDestructor
		MyComparatorCompare,
		MyComparatorName);
}
*/
import "C"

type Comparator struct {
	comparator *C.leveldb_comparator_t
}

func NewMyComparator() *Comparator {
	return &Comparator{C.NewMyComparator()}
}

func (cmp *Comparator) Destroy() {
	C.leveldb_comparator_destroy(cmp.comparator)
	cmp.comparator = nil
}

func main() {
	NewMyComparator().Destroy()
}
