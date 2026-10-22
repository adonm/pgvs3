package s3

import "net/http"

// validateSSE accepts the encryption modes this gateway can represent. Bytes
// are stored in PostgreSQL; SSE-C and DSSE-KMS are rejected.
func validateSSE(r *http.Request) error {
	if r.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key-md5") != "" {
		return errNotImplemented()
	}
	switch enc := r.Header.Get("x-amz-server-side-encryption"); enc {
	case "", "AES256", "aws:kms":
		return nil
	case "aws:kms:dsse":
		return errNotImplemented()
	default:
		return errInvalidRequest("unsupported server-side encryption")
	}
}
