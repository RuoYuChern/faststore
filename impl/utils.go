package impl

func bcopy(dst, src []byte, dOff, sOff, bLen uint32) {
	for o := uint32(0); o < bLen; o++ {
		dst[o] = src[o]
	}
}
