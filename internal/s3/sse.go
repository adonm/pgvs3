package s3

import "net/http"

// validateWriteHeaders enforces the object-header policy for write requests.
// The compatibility tier accepts AES256 and a private ACL as documented no-ops
// (at-rest encryption is delegated to PostgreSQL), and rejects features that
// would otherwise look enforced but are not: KMS, public ACLs/grants, Object
// Lock, tagging, and non-STANDARD storage classes.
func validateWriteHeaders(r *http.Request) error {
	if err := validateSSE(r); err != nil {
		return err
	}
	switch acl := r.Header.Get("x-amz-acl"); acl {
	case "", "private":
	default:
		return errNotImplemented()
	}
	for _, h := range []string{
		"x-amz-grant-read",
		"x-amz-grant-write",
		"x-amz-grant-read-acp",
		"x-amz-grant-write-acp",
		"x-amz-grant-full-control",
		"x-amz-object-lock-mode",
		"x-amz-object-lock-retain-until-date",
		"x-amz-object-lock-legal-hold",
		"x-amz-tagging",
	} {
		if r.Header.Get(h) != "" {
			return errNotImplemented()
		}
	}
	switch class := r.Header.Get("x-amz-storage-class"); class {
	case "", "STANDARD":
	default:
		return errNotImplemented()
	}
	return nil
}

// validateSSE accepts the encryption modes this gateway can represent. Bytes
// are never encrypted by the gateway; AES256 is therefore a no-op that is only
// honest when the PostgreSQL volume is encrypted at rest. KMS and SSE-C imply
// guarantees pgvs3 cannot provide and are rejected.
func validateSSE(r *http.Request) error {
	if r.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key-md5") != "" {
		return errNotImplemented()
	}
	for _, h := range []string{
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-server-side-encryption-context",
		"x-amz-server-side-encryption-bucket-key-enabled",
	} {
		if r.Header.Get(h) != "" {
			return errNotImplemented()
		}
	}
	switch enc := r.Header.Get("x-amz-server-side-encryption"); enc {
	case "", "AES256":
		return nil
	case "aws:kms", "aws:kms:dsse":
		return errNotImplemented()
	default:
		return errInvalidRequest("unsupported server-side encryption")
	}
}
