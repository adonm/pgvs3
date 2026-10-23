package main

import (
	"reflect"
	"testing"
)

func TestExtractSubcommand(t *testing.T) {
	cases := []struct {
		in       []string
		wantCmd  string
		wantRest []string
	}{
		{
			in:       []string{"drop", "--force", "stat"},
			wantCmd:  "drop",
			wantRest: []string{"--force", "stat"},
		},
		{
			in:       []string{"clone", "migrate", "doctor"},
			wantCmd:  "clone",
			wantRest: []string{"migrate", "doctor"},
		},
		{
			in:       []string{"--url", "postgres://localhost/pgvs3", "serve", "--addr", "127.0.0.1:8014"},
			wantCmd:  "serve",
			wantRest: []string{"--url", "postgres://localhost/pgvs3", "--addr", "127.0.0.1:8014"},
		},
		{
			in:       []string{"--addr", "127.0.0.1:8014"},
			wantCmd:  "",
			wantRest: []string{"--addr", "127.0.0.1:8014"},
		},
	}
	for _, tc := range cases {
		cmd, rest := extractSubcommand(tc.in)
		if cmd != tc.wantCmd || !reflect.DeepEqual(rest, tc.wantRest) {
			t.Fatalf("extractSubcommand(%v) = (%q, %v), want (%q, %v)", tc.in, cmd, rest, tc.wantCmd, tc.wantRest)
		}
	}
}
