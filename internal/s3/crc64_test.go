package s3

import "testing"

func TestCRC64NVMEVector(t *testing.T) {
	c := newCRC64NVME()
	_, _ = c.Write([]byte("123456789"))
	if got, want := c.Sum64(), uint64(0xAE8B14860A799888); got != want {
		t.Fatalf("crc64nvme = %#x, want %#x", got, want)
	}
}
