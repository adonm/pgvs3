package reqguard

import (
	"net/url"
	"strings"
)

const PresignRejection = "presigned URLs are not supported: pgvs3 does not authenticate requests and must run behind network-level access control"

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

	return sigv4Signature || sigv4Algorithm || sigv4Credential || (legacyKey && legacySignature)
}
