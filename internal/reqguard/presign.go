// Package reqguard holds request-level policy checks that are independent of
// any one protocol's wire format.
package reqguard

import (
	"net/url"
	"strings"
)

// PresignRejection is the client-facing explanation used when a request
// carries presigned-URL credentials.
const PresignRejection = "presigned URLs are not supported: pgvs3 does not authenticate requests and must run behind network-level access control"

// Presigned reports whether the query string carries authentication
// parameters, i.e. the request is a presigned URL.
//
// pgvs3 deliberately does not validate credentials and relies on network-level
// trust. Accepting a presigned URL would turn a scoped, time-limited link into
// an unscoped grant on the whole gateway, so presigned requests are rejected
// instead of silently ignored.
//
// The check covers SigV4 and SigV4a query authentication and the legacy SigV2
// query form. Header-based SigV4 (Authorization: AWS4-HMAC-SHA256 ...) is not
// a presigned URL and continues to be accepted and ignored.
func Presigned(q url.Values) bool {
	var sigv4Algorithm, sigv4Credential, sigv4Signature bool
	var legacyKey, legacySignature bool
	for k := range q {
		switch {
		case strings.EqualFold(k, "X-Amz-Algorithm"):
			sigv4Algorithm = true
		case strings.EqualFold(k, "X-Amz-Credential"):
			sigv4Credential = true
		case strings.EqualFold(k, "X-Amz-Signature"):
			sigv4Signature = true
		case strings.EqualFold(k, "AWSAccessKeyId"):
			legacyKey = true
		case strings.EqualFold(k, "Signature"):
			legacySignature = true
		}
	}
	// X-Amz-Algorithm and X-Amz-Credential only appear in query-string
	// authentication; X-Amz-Signature is the definitive SigV4 marker.
	return sigv4Signature || sigv4Algorithm || sigv4Credential || (legacyKey && legacySignature)
}
