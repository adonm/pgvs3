package main

import "testing"

func TestParseBytes(t *testing.T) {
	for in, want := range map[string]int64{"10MiB": 10 << 20, "1GiB": 1 << 30, "8120B": 8120, " 4KiB ": 4096} {
		if got, err := parseBytes(in); err != nil || got != want {
			t.Fatalf("parseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"10", "0MiB", "-1GiB", "xMiB"} {
		if _, err := parseBytes(bad); err == nil {
			t.Fatalf("parseBytes(%q) accepted", bad)
		}
	}
}

func TestChunkSourceMatchesStoreLayout(t *testing.T) {
	body := make([]byte, 2*rowBytes+1)
	src := &chunkSource{fileID: 7, body: body}
	var sizes []int
	for src.Next() {
		v, _ := src.Values()
		if v[0].(int64) != 7 || int(v[1].(int32)) != len(sizes) {
			t.Fatalf("row %d values %v", len(sizes), v[:2])
		}
		sizes = append(sizes, len(v[2].([]byte)))
	}
	if len(sizes) != 3 || sizes[0] != rowBytes || sizes[1] != rowBytes || sizes[2] != 1 {
		t.Fatalf("row sizes %v", sizes)
	}
}

func TestSummarizeUsesSlowerFloorAndMedian(t *testing.T) {
	var samples []sample
	add := func(impl string, rates ...float64) {
		for _, r := range rates {
			samples = append(samples, sample{Size: 1, Op: "get", Impl: impl, MiBps: r, CPUNsPer: r / 100})
		}
	}
	add("pgvs3", 90, 100, 500)
	add("http", 1000, 1000, 1000)
	add("pg", 190, 200, 210)
	add("pg-1conn", 50, 50, 50)
	var got row
	for _, r := range summarize(samples, []int64{1}) {
		if r.Op == "get" {
			got = r
		}
	}
	if got.PGVS3 != 100 || got.Floor != 200 || got.PercentFloor != 50 || got.PGOneConn != 50 || got.CPUNsPerByte != 1 {
		t.Fatalf("summary %+v", got)
	}
}
