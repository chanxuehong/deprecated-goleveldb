package goleveldb

import "C"

func bool2uchar(b bool) C.uchar {
	uc := C.uchar(0)
	if b {
		uc = C.uchar(1)
	}
	return uc
}

func uchar2bool(uc C.uchar) bool {
	if uc == C.uchar(0) {
		return false
	}
	return true
}
