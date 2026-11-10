package s3

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func buildSeekableZstd(t *testing.T, data []byte, blockSize int) []byte {
	t.Helper()
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	type frame struct {
		compressed []byte
		uncompSize int
	}
	var frames []frame
	for off := 0; off < len(data); off += blockSize {
		end := min(off+blockSize, len(data))
		frames = append(frames, frame{compressed: enc.EncodeAll(data[off:end], nil), uncompSize: end - off})
	}
	var table bytes.Buffer
	var word [4]byte
	for _, f := range frames {
		binary.LittleEndian.PutUint32(word[:], uint32(len(f.compressed)))
		table.Write(word[:])
		binary.LittleEndian.PutUint32(word[:], uint32(f.uncompSize))
		table.Write(word[:])
	}
	binary.LittleEndian.PutUint32(word[:], uint32(len(frames)))
	table.Write(word[:])
	table.WriteByte(0)
	binary.LittleEndian.PutUint32(word[:], zstdSeekMagic)
	table.Write(word[:])

	var out bytes.Buffer
	for _, f := range frames {
		out.Write(f.compressed)
	}
	binary.LittleEndian.PutUint32(word[:], zstdSkippableMagic)
	out.Write(word[:])
	binary.LittleEndian.PutUint32(word[:], uint32(table.Len()))
	out.Write(word[:])
	out.Write(table.Bytes())
	return out.Bytes()
}

func buildZipWithCompressed(t *testing.T, name string, method uint16, compressed, plain []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	var hdr [30]byte
	binary.LittleEndian.PutUint32(hdr[0:], zipLocalSignature)
	binary.LittleEndian.PutUint16(hdr[4:], 45)
	binary.LittleEndian.PutUint16(hdr[8:], method)
	binary.LittleEndian.PutUint32(hdr[14:], crc32.ChecksumIEEE(plain))
	binary.LittleEndian.PutUint32(hdr[18:], uint32(len(compressed)))
	binary.LittleEndian.PutUint32(hdr[22:], uint32(len(plain)))
	binary.LittleEndian.PutUint16(hdr[26:], uint16(len(name)))
	out.Write(hdr[:])
	out.WriteString(name)
	out.Write(compressed)
	cdStart := out.Len()
	var cd [46]byte
	binary.LittleEndian.PutUint32(cd[0:], zipCentralSig)
	binary.LittleEndian.PutUint16(cd[4:], 45)
	binary.LittleEndian.PutUint16(cd[6:], 45)
	binary.LittleEndian.PutUint16(cd[10:], method)
	binary.LittleEndian.PutUint32(cd[16:], crc32.ChecksumIEEE(plain))
	binary.LittleEndian.PutUint32(cd[20:], uint32(len(compressed)))
	binary.LittleEndian.PutUint32(cd[24:], uint32(len(plain)))
	binary.LittleEndian.PutUint16(cd[28:], uint16(len(name)))
	out.Write(cd[:])
	out.WriteString(name)
	cdSize := out.Len() - cdStart
	var eocd [22]byte
	binary.LittleEndian.PutUint32(eocd[0:], zipEndSignature)
	binary.LittleEndian.PutUint16(eocd[8:], 1)
	binary.LittleEndian.PutUint16(eocd[10:], 1)
	binary.LittleEndian.PutUint32(eocd[12:], uint32(cdSize))
	binary.LittleEndian.PutUint32(eocd[16:], uint32(cdStart))
	out.Write(eocd[:])
	return out.Bytes()
}

func buildTar(t *testing.T, files []zipTestFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range files {
		hdr := &tar.Header{
			Name: f.name, Mode: 0o644, Size: int64(len(f.body)),
			ModTime: time.Unix(1_700_000_000, 0), Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("tar header %s: %v", f.name, err)
		}
		if _, err := tw.Write(f.body); err != nil {
			t.Fatalf("tar body %s: %v", f.name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	return buf.Bytes()
}

func TestSplitArchiveKey(t *testing.T) {
	cases := []struct {
		key, archive, member string
		kind                 archiveKind
		ok                   bool
	}{
		{"b.zip/a/b", "b.zip", "a/b", archiveZip, true},
		{"b.ZIP/a", "b.ZIP", "a", archiveZip, true},
		{"b.tar/a", "b.tar", "a", archiveTar, true},
		{"b.TAR/dir/a", "b.TAR", "dir/a", archiveTar, true},
		{"b.tar.zst/a", "b.tar.zst", "a", archiveTarZstd, true},
		{"b.TAR.ZST/a", "b.TAR.ZST", "a", archiveTarZstd, true},
		{"b.tzst/a", "b.tzst", "a", archiveTarZstd, true},
		{"b.tar.gz/a", "", "", archiveZip, false},
		{"b.zip/../a", "", "", archiveZip, false},
	}
	for _, tc := range cases {
		archive, member, kind, ok := splitArchiveKey(tc.key)
		if archive != tc.archive || member != tc.member || kind != tc.kind || ok != tc.ok {
			t.Fatalf("splitArchiveKey(%q) = (%q,%q,%d,%v), want (%q,%q,%d,%v)",
				tc.key, archive, member, kind, ok, tc.archive, tc.member, tc.kind, tc.ok)
		}
	}
}

func TestTarVirtualFolder(t *testing.T) {
	parquet := bytes.Repeat([]byte("PAR1-tar-member"), 20_000)
	quant := []byte("transcript\tlength\tTPM\ntx1\t100\t1.5\n")
	filler := bytes.Repeat([]byte{0xa5}, 2<<20)
	archive := buildTar(t, []zipTestFile{
		{name: "data/filler.bin", body: filler},
		{name: "data/events.parquet", body: parquet},
		{name: "quant.sf", body: quant},
		{name: "meta/columns.json", body: []byte(`{"columns":3}`)},
	})
	backend := newZipTestBackend()
	backend.put("bundle.tar", archive)
	_, ts := newZipTestServer(t, backend)

	resp, _ := zipHTTP(t, ts, http.MethodHead, "/bucket/bundle.tar/quant.sf", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != "34" {
		t.Fatalf("HEAD status=%d length=%q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}
	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/bundle.tar/quant.sf", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, quant) {
		t.Fatalf("GET status=%d body=%q", resp.StatusCode, body)
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/bundle.tar/data/events.parquet", map[string]string{"Range": "bytes=100-499"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[100:500]) {
		t.Fatalf("range status=%d len=%d", resp.StatusCode, len(body))
	}
	out := listZip(t, ts, "bucket", "prefix=bundle.tar/&delimiter=/")
	if len(out.CommonPrefixes) != 2 || out.CommonPrefixes[0].Prefix != "bundle.tar/data/" || out.CommonPrefixes[1].Prefix != "bundle.tar/meta/" {
		t.Fatalf("prefixes = %+v", out.CommonPrefixes)
	}
	if len(out.Contents) != 1 || out.Contents[0].Key != "bundle.tar/quant.sf" {
		t.Fatalf("contents = %+v", out.Contents)
	}

	// Indexing must hop headers, not stream multi-hundred-KiB member bodies.
	var read int64
	for _, op := range backend.opensFor("bundle.tar") {
		read += op.last - op.first + 1
	}
	if read >= int64(len(archive))/2 {
		t.Fatalf("tar index read %d of %d bytes; header hops read member bodies", read, len(archive))
	}
}

func TestSeekableTarZstdVirtualFolder(t *testing.T) {
	parquet := bytes.Repeat([]byte("PAR1-tar-zstd-member"), 15_000)
	quant := []byte("transcript\tlength\tTPM\ntx1\t100\t1.5\n")
	tarball := buildTar(t, []zipTestFile{
		{name: "data/events.parquet", body: parquet},
		{name: "data/events2.parquet", body: parquet[:1000]},
		{name: "quant.sf", body: quant},
		{name: "meta/columns.json", body: []byte(`{"columns":3}`)},
	})
	archive := buildSeekableZstd(t, tarball, 32<<10)
	if table, required, ok := parseZstdSeekTable(archive, int64(len(archive))); !ok {
		ts := len(archive) - int(required)
		t.Fatalf("built seekable zstd archive does not parse (%d bytes): required=%d start=%d magic=%x frames=%d desc=%x",
			len(archive), required, ts, binary.LittleEndian.Uint32(archive[ts:]), binary.LittleEndian.Uint32(archive[len(archive)-9:]), archive[len(archive)-5])
	} else if table.uncompressedSize != int64(len(tarball)) {
		t.Fatalf("uncompressed size = %d, want %d", table.uncompressedSize, len(tarball))
	}
	backend := newZipTestBackend()
	backend.put("bundle.tar.zst", archive)
	srv, ts := newZipTestServer(t, backend)
	if _, _, err := srv.loadZipIndex(context.Background(), backend, "bucket", "bundle.tar.zst"); err != nil {
		t.Fatalf("build tar.zst index: %v", err)
	}

	resp, _ := zipHTTP(t, ts, http.MethodHead, "/bucket/bundle.tar.zst/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != "300000" {
		t.Fatalf("HEAD status=%d length=%q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}
	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/bundle.tar.zst/quant.sf", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, quant) {
		t.Fatalf("GET status=%d body=%q", resp.StatusCode, body)
	}
	backend.resetOps()
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/bundle.tar.zst/data/events.parquet", map[string]string{"Range": "bytes=100000-100999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[100000:101000]) {
		t.Fatalf("range status=%d len=%d", resp.StatusCode, len(body))
	}
	ops := backend.opensFor("bundle.tar.zst")
	if len(ops) == 0 {
		t.Fatal("range read opened no archive bytes")
	}
	for _, op := range ops {
		if op.first == 0 {
			t.Fatalf("seekable range read started at compressed offset 0: %+v", ops)
		}
	}
	out := listZip(t, ts, "bucket", "prefix=bundle.tar.zst/&delimiter=/")
	if len(out.CommonPrefixes) != 2 || out.CommonPrefixes[0].Prefix != "bundle.tar.zst/data/" || out.CommonPrefixes[1].Prefix != "bundle.tar.zst/meta/" {
		t.Fatalf("prefixes = %+v", out.CommonPrefixes)
	}
	if len(out.Contents) != 1 || out.Contents[0].Key != "bundle.tar.zst/quant.sf" {
		t.Fatalf("contents = %+v", out.Contents)
	}

	backend.put("bundle.tzst", archive)
	resp, _ = zipHTTP(t, ts, http.MethodHead, "/bucket/bundle.tzst/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("tzst HEAD status=%d", resp.StatusCode)
	}
}

func TestSeekableZstdZipRandomAccess(t *testing.T) {
	data := make([]byte, 17<<20)
	rnd := rand.New(rand.NewSource(7))
	_, _ = rnd.Read(data)
	compressed := buildSeekableZstd(t, data, 256<<10)
	archive := buildZipWithCompressed(t, "data/events.parquet", zstd.ZipMethodWinZip, compressed, data)
	backend := newZipTestBackend()
	backend.put("batch.zip", archive)
	_, ts := newZipTestServer(t, backend)

	resp, _ := zipHTTP(t, ts, http.MethodHead, "/bucket/batch.zip/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != "17825792" {
		t.Fatalf("HEAD status=%d length=%q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}

	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatal(err)
	}
	memberStart, err := zr.File[0].DataOffset()
	if err != nil {
		t.Fatal(err)
	}

	// Warm the seek table and the member data offset.
	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/batch.zip/data/events.parquet", map[string]string{"Range": "bytes=0-99"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[:100]) {
		t.Fatalf("warm range status=%d len=%d", resp.StatusCode, len(body))
	}
	backend.resetOps()
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/batch.zip/data/events.parquet", map[string]string{"Range": "bytes=12000000-12000999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[12000000:12001000]) {
		t.Fatalf("middle range status=%d len=%d", resp.StatusCode, len(body))
	}
	ops := backend.opensFor("batch.zip")
	if len(ops) == 0 {
		t.Fatal("seekable zstd range read opened no archive bytes")
	}
	for _, op := range ops {
		if op.first <= memberStart {
			t.Fatalf("seekable zstd range read included frame 0: memberStart=%d ops=%+v", memberStart, ops)
		}
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/batch.zip/data/events.parquet", map[string]string{"Range": "bytes=17000000-17000999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[17000000:17001000]) {
		t.Fatalf("tail range status=%d len=%d", resp.StatusCode, len(body))
	}
}
