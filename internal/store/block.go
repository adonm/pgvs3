package store

import "fmt"

// compressBlock compresses src into dst using the raw LZ4 block format.
// If src is incompressible (compressed size >= len(src)) or larger than 64 KiB,
// it returns src unchanged so callers can distinguish compressed vs raw blocks
// purely by comparing len(stored) < logicalLen with zero header bytes.
func compressBlock(dst, src []byte) []byte {
	n := len(src)
	if n < 16 || n > MaxInlineBytes {
		return src
	}
	maxOut := n - 1
	if cap(dst) < maxOut {
		dst = make([]byte, maxOut)
	} else {
		dst = dst[:maxOut]
	}

	var table [4096]uint16
	const (
		minMatch = 4
		endLit   = 5
		mfLimit  = 12
	)
	if n < mfLimit {
		return src
	}

	hash4 := func(i int) uint32 {
		v := uint32(src[i]) | uint32(src[i+1])<<8 | uint32(src[i+2])<<16 | uint32(src[i+3])<<24
		return (v * 2654435761) >> 20
	}

	var (
		anchor = 0
		si     = 0
		di     = 0
		sLimit = n - mfLimit
	)
	table[hash4(0)] = 0
	si = 1

	for si <= sLimit {
		var (
			ref  int
			step = 1
			next = si
		)
		// Find a 4-byte match with accelerating skip on incompressible runs.
		for {
			si = next
			if si > sLimit {
				goto lastLiterals
			}
			h := hash4(si)
			ref = int(table[h])
			table[h] = uint16(si)
			next = si + step
			step = 1 + ((si - anchor) >> 5)
			if ref < si && si-ref <= 0xffff &&
				src[ref] == src[si] && src[ref+1] == src[si+1] &&
				src[ref+2] == src[si+2] && src[ref+3] == src[si+3] {
				break
			}
		}

		// Extend match backwards if possible.
		for si > anchor && ref > 0 && src[si-1] == src[ref-1] {
			si--
			ref--
		}

		litLen := si - anchor
		tokenPos := di
		di++
		if di >= maxOut {
			return src
		}
		if litLen >= 15 {
			rem := litLen - 15
			for rem >= 255 {
				if di >= maxOut {
					return src
				}
				dst[di] = 255
				di++
				rem -= 255
			}
			if di >= maxOut {
				return src
			}
			dst[di] = byte(rem)
			di++
		}
		if di+litLen > maxOut {
			return src
		}
		copy(dst[di:], src[anchor:si])
		di += litLen

		// Measure match length past the initial 4 bytes.
		matchLen := minMatch
		maxMatch := n - endLit - si
		for matchLen < maxMatch && src[ref+matchLen] == src[si+matchLen] {
			matchLen++
		}

		if di+2 > maxOut {
			return src
		}
		offset := si - ref
		dst[di] = byte(offset)
		dst[di+1] = byte(offset >> 8)
		di += 2

		mCode := matchLen - minMatch
		if mCode >= 15 {
			dst[tokenPos] = byte(min(litLen, 15)<<4 | 15)
			rem := mCode - 15
			for rem >= 255 {
				if di >= maxOut {
					return src
				}
				dst[di] = 255
				di++
				rem -= 255
			}
			if di >= maxOut {
				return src
			}
			dst[di] = byte(rem)
			di++
		} else {
			dst[tokenPos] = byte(min(litLen, 15)<<4 | mCode)
		}

		si += matchLen
		anchor = si
		if si <= sLimit {
			table[hash4(si-2)] = uint16(si - 2)
		}
	}

lastLiterals:
	litLen := n - anchor
	if di+1+litLen+(litLen/255)+1 > maxOut {
		return src
	}
	tokenPos := di
	di++
	if litLen >= 15 {
		dst[tokenPos] = 15 << 4
		rem := litLen - 15
		for rem >= 255 {
			dst[di] = 255
			di++
			rem -= 255
		}
		dst[di] = byte(rem)
		di++
	} else {
		dst[tokenPos] = byte(litLen << 4)
	}
	copy(dst[di:], src[anchor:])
	di += litLen
	return dst[:di]
}

// decompressBlock decodes a block produced by compressBlock into wantLen bytes.
// If len(src) == wantLen, src was stored uncompressed and is returned as-is.
func decompressBlock(dst, src []byte, wantLen int) ([]byte, error) {
	if wantLen == 0 {
		return []byte{}, nil
	}
	if len(src) == wantLen {
		return src, nil
	}
	if len(src) > wantLen {
		return nil, fmt.Errorf("corrupt block: stored %d > logical %d", len(src), wantLen)
	}
	if cap(dst) < wantLen {
		dst = make([]byte, wantLen)
	} else {
		dst = dst[:wantLen]
	}
	var si, di int
	for si < len(src) {
		token := src[si]
		si++
		litLen := int(token >> 4)
		if litLen == 15 {
			for {
				if si >= len(src) {
					return nil, fmt.Errorf("corrupt block: truncated literal length")
				}
				b := src[si]
				si++
				litLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		if si+litLen > len(src) || di+litLen > wantLen {
			return nil, fmt.Errorf("corrupt block: literal overflow")
		}
		copy(dst[di:di+litLen], src[si:si+litLen])
		si += litLen
		di += litLen
		if si == len(src) {
			break
		}
		if si+2 > len(src) {
			return nil, fmt.Errorf("corrupt block: truncated offset")
		}
		offset := int(src[si]) | int(src[si+1])<<8
		si += 2
		if offset <= 0 || offset > di {
			return nil, fmt.Errorf("corrupt block: invalid offset %d at %d", offset, di)
		}
		matchLen := int(token&0x0f) + 4
		if matchLen == 19 {
			for {
				if si >= len(src) {
					return nil, fmt.Errorf("corrupt block: truncated match length")
				}
				b := src[si]
				si++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		if di+matchLen > wantLen {
			return nil, fmt.Errorf("corrupt block: match overflow")
		}
		ref := di - offset
		if offset >= matchLen {
			copy(dst[di:di+matchLen], dst[ref:ref+matchLen])
			di += matchLen
		} else {
			for i := 0; i < matchLen; i++ {
				dst[di] = dst[ref+i]
				di++
			}
		}
	}
	if di != wantLen {
		return nil, fmt.Errorf("corrupt block: decoded %d bytes, want %d", di, wantLen)
	}
	return dst, nil
}
