package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUniqueReadSequenceAcrossWorkers(t *testing.T) {
	for _, unique := range []bool{false, true} {
		t.Run(fmt.Sprint(unique), func(t *testing.T) {
			paths := make(chan string, 12)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths <- r.URL.Path
				_, _ = io.WriteString(w, "body")
			}))
			defer server.Close()
			s := &s3run{bucket: "bench", prefix: "objects", count: 12, uniqueReads: unique, base: server.URL, client: server.Client()}
			factory := s.objects(http.MethodGet, "", http.StatusOK)
			workers := make([]worker, 4)
			for i := range workers {
				var err error
				workers[i], err = factory(i)
				if err != nil {
					t.Fatal(err)
				}
			}
			seen := map[string]bool{}
			for _, id := range []int{0, 0, 1, 0, 2, 3, 3, 1, 1, 2, 0, 3} {
				if _, _, err := workers[id].step(); err != nil {
					t.Fatal(err)
				}
				seen[<-paths] = true
			}
			if (len(seen) == 12) != unique {
				t.Fatalf("unique=%t paths=%v", unique, seen)
			}
		})
	}
}
