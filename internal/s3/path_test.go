package s3

import "testing"

func TestAWSEncode(t *testing.T) {
	tests := []struct {
		in          string
		encodeSlash bool
		want        string
	}{
		{"abc-._~09AZ", true, "abc-._~09AZ"},
		{"a b", true, "a%20b"},
		{"a/b", true, "a%2Fb"},
		{"a/b", false, "a/b"},
		{"a+b", true, "a%2Bb"},
	}
	for _, tt := range tests {
		if got := awsEncode(tt.in, tt.encodeSlash); got != tt.want {
			t.Errorf("awsEncode(%q, %v) = %q, want %q", tt.in, tt.encodeSlash, got, tt.want)
		}
	}
}
