package impl

func bcopy(dst, src []byte, dOff, sOff, bLen int) {
	for o := 0; o < bLen; o++ {
		dst[o] = src[o]
	}
}
