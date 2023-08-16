package impl

func duplicate(src []byte) []byte {
	l := len(src)
	dst := make([]byte, l)
	bcopy(dst, src, 0, 0, uint32(l))
	return dst
}

func bcopy(dst, src []byte, dOff, sOff, bLen uint32) {
	for o := uint32(0); o < bLen; o++ {
		dst[dOff+o] = src[sOff+o]
	}
}
