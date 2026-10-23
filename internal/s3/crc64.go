package s3

import "encoding/binary"

// CRC-64/NVME (reflected polynomial 0xAD93D23594C93659), used by current AWS
// SDKs as the default checksum algorithm.
var crc64NVMETable = func() [256]uint64 {
	const poly = 0x9a6c9329ac4bc9b5 // reflected form of 0xAD93D23594C93659
	var t [256]uint64
	for i := range t {
		crc := uint64(i)
		for range 8 {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		t[i] = crc
	}
	return t
}()

type crc64NVME struct {
	crc uint64
}

func newCRC64NVME() *crc64NVME { return &crc64NVME{crc: ^uint64(0)} }

func (c *crc64NVME) Write(p []byte) (int, error) {
	for _, b := range p {
		c.crc = crc64NVMETable[byte(c.crc)^b] ^ (c.crc >> 8)
	}
	return len(p), nil
}

func (c *crc64NVME) Sum64() uint64 { return c.crc ^ ^uint64(0) }

func (c *crc64NVME) Sum(in []byte) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], c.Sum64())
	return append(in, b[:]...)
}

func (c *crc64NVME) Reset()         { c.crc = ^uint64(0) }
func (c *crc64NVME) Size() int      { return 8 }
func (c *crc64NVME) BlockSize() int { return 1 }
