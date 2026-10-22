package store

import (
	"bytes"
	"testing"
)

func FuzzRowFramer(f *testing.F) {
	f.Add([]byte("hello"), int64(1))
	f.Add(bytes.Repeat([]byte("x"), RowBytes+1), int64(42))
	f.Fuzz(func(t *testing.T, data []byte, id int64) {
		if len(data) > 4*RowBytes || id < 0 {
			return
		}
		fr := newRowFramer(id)
		if _, err := fr.push(data); err != nil {
			return
		}
		fr.finish()
	})
}
