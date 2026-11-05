package store

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestPartsBodyRead(t *testing.T) {
	body := NewPartsBody([][]byte{[]byte("hello "), []byte("world")})
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("ReadAll = %q, want %q", got, "hello world")
	}
}

func TestPartsBodyWriteTo(t *testing.T) {
	body := NewPartsBody([][]byte{[]byte("hello "), nil, []byte("world")})
	var out bytes.Buffer
	n, err := body.WriteTo(&out)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != int64(out.Len()) || out.String() != "hello world" {
		t.Fatalf("WriteTo = %d %q, want %d %q", n, out.String(), len("hello world"), "hello world")
	}
}

func TestPartsBodyPartialReadThenWriteTo(t *testing.T) {
	body := NewPartsBody([][]byte{[]byte("hello "), []byte("world")})
	head := make([]byte, 4)
	if n, err := io.ReadFull(body, head); n != len(head) || err != nil {
		t.Fatalf("ReadFull = %d, %v", n, err)
	}
	var rest bytes.Buffer
	n, err := body.WriteTo(&rest)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != int64(rest.Len()) || rest.String() != "o world" {
		t.Fatalf("WriteTo = %d %q, want %d %q", n, rest.String(), len("o world"), "o world")
	}
}

func TestPartsBodyEmpty(t *testing.T) {
	body := NewPartsBody(nil)
	if n, err := body.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("Read = %d, %v, want 0, EOF", n, err)
	}
	n, err := body.WriteTo(io.Discard)
	if n != 0 || err != nil {
		t.Fatalf("WriteTo = %d, %v, want 0, nil", n, err)
	}
}

func TestPartsBodyClose(t *testing.T) {
	body := NewPartsBody([][]byte{[]byte("hello")})
	if err := body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := body.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestPartsBodyWriteToError(t *testing.T) {
	boom := errors.New("boom")
	body := NewPartsBody([][]byte{[]byte("hello"), []byte("world")})
	n, err := body.WriteTo(&limitedWriter{n: 2, err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("WriteTo error = %v, want %v", err, boom)
	}
	if n != 2 {
		t.Fatalf("WriteTo wrote %d bytes, want 2", n)
	}
}

func TestPartsBodyWriteToShortWrite(t *testing.T) {
	body := NewPartsBody([][]byte{[]byte("hello")})
	n, err := body.WriteTo(shortWriter{})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteTo error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 4 {
		t.Fatalf("WriteTo wrote %d bytes, want 4", n)
	}
}

type limitedWriter struct {
	n   int
	err error
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.n)
	w.n -= n
	return n, w.err
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	return len(p) - 1, nil
}

func BenchmarkPartsBodyWriteTo(b *testing.B) {
	cases := []struct {
		name     string
		partSize int
		total    int
	}{
		{name: "64KiB-parts/1MiB", partSize: 64 << 10, total: 1 << 20},
		{name: "64KiB-parts/8MiB", partSize: 64 << 10, total: 8 << 20},
		{name: "1MiB-parts/1MiB", partSize: 1 << 20, total: 1 << 20},
		{name: "1MiB-parts/8MiB", partSize: 1 << 20, total: 8 << 20},
	}
	for _, tc := range cases {
		parts := benchParts(tc.total, tc.partSize)
		total := int64(tc.total)
		b.Run(tc.name+"/oldMultiReader", func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()
			for b.Loop() {
				readers := make([]io.Reader, len(parts))
				for i := range parts {
					readers[i] = bytes.NewReader(parts[i])
				}
				body := io.NopCloser(io.MultiReader(readers...))
				n, err := io.Copy(io.Discard, body)
				if err != nil || n != total {
					b.Fatalf("Copy = %d, %v, want %d, nil", n, err, total)
				}
			}
		})
		b.Run(tc.name+"/partsBody", func(b *testing.B) {
			b.SetBytes(total)
			b.ReportAllocs()
			for b.Loop() {
				borrowed := make([][]byte, len(parts))
				copy(borrowed, parts)
				body := NewPartsBody(borrowed)
				n, err := io.Copy(io.Discard, body)
				if err != nil || n != total {
					b.Fatalf("Copy = %d, %v, want %d, nil", n, err, total)
				}
			}
		})
	}
}

func benchParts(size, partSize int) [][]byte {
	parts := make([][]byte, size/partSize)
	for i := range parts {
		parts[i] = make([]byte, partSize)
	}
	return parts
}
