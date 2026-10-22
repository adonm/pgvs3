package s3

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

// splitPath accepts path-style and virtual-hosted-style requests. Virtual-host
// routing is enabled only when --vhost-domain is configured.
func splitPath(r *http.Request, vhostDomain string) (string, string, error) {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if vhostDomain != "" && (host == vhostDomain || strings.HasSuffix(host, "."+vhostDomain)) {
		first := strings.TrimSuffix(host, "."+vhostDomain)
		if first != host && first != "" && !strings.Contains(first, ".") {
			key, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/"))
			if err != nil {
				return "", "", err
			}
			return first, key, nil
		}
	}
	decoded, err := url.PathUnescape(r.URL.EscapedPath())
	if err != nil {
		return "", "", err
	}
	decoded = strings.TrimPrefix(decoded, "/")
	if decoded == "" {
		return "", "", nil
	}
	bucket, key, _ := strings.Cut(decoded, "/")
	return bucket, key, nil
}

// awsEncode is the AWS URI encoding used by ListObjectsV2 encoding-type=url.
func awsEncode(s string, encodeSlash bool) string {
	const hexChars = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexChars[c>>4])
			b.WriteByte(hexChars[c&15])
		}
	}
	return b.String()
}
