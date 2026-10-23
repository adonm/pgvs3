// Package ns defines the namespace model shared by every protocol pgvs3
// serves: object buckets, volatile cache databases and message queues.
package ns

import "errors"

// Kind is the class of namespace. It selects the PostgreSQL schema prefix,
// table persistence (WAL-logged or UNLOGGED) and the inline value ceiling.
type Kind string

const (
	// Bucket is a durable S3 bucket (WAL-logged, 64 KiB inline objects).
	Bucket Kind = "bucket"
	// Cache is a volatile key/value namespace (UNLOGGED, one page per value).
	Cache Kind = "cache"
	// Queue is an SQS queue (WAL-logged, one page per message body).
	Queue Kind = "queue"
)

// ErrInvalidName is returned for names that cannot be mapped to a schema.
var ErrInvalidName = errors.New("invalid namespace name")

// ErrUnknownKind is returned for an unrecognised Kind.
var ErrUnknownKind = errors.New("unknown namespace kind")

// Valid reports whether k is a known kind.
func (k Kind) Valid() bool {
	switch k {
	case Bucket, Cache, Queue:
		return true
	}
	return false
}

// Prefix is the PostgreSQL schema prefix for the kind.
func (k Kind) Prefix() string {
	switch k {
	case Cache:
		return "s3c_"
	case Queue:
		return "s3q_"
	default:
		return "s3b_"
	}
}

// MaxNameLen is the longest logical name that still fits a 63-byte identifier.
const MaxNameLen = 58

// Schema validates name and returns the PostgreSQL schema for it.
func (k Kind) Schema(name string) (string, error) {
	if !k.Valid() {
		return "", ErrUnknownKind
	}
	if !ValidName(name) {
		return "", ErrInvalidName
	}
	return k.Prefix() + name, nil
}

// ValidName reports whether name is a legal S3-style namespace name.
func ValidName(name string) bool {
	if len(name) == 0 || len(name) > MaxNameLen {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
		default:
			return false
		}
	}
	return true
}

// ParseSchema splits a `s3?_<name>` schema back into its kind and name.
func ParseSchema(schema string) (Kind, string, bool) {
	var k Kind
	switch {
	case len(schema) > 4 && schema[:4] == "s3b_":
		k = Bucket
	case len(schema) > 4 && schema[:4] == "s3c_":
		k = Cache
	case len(schema) > 4 && schema[:4] == "s3q_":
		k = Queue
	default:
		return "", "", false
	}
	name := schema[4:]
	if !ValidName(name) {
		return "", "", false
	}
	return k, name, true
}

// InlineMax is the largest value kept in a single inline row for the kind.
// Cache and queue values are capped at one 8 KB page so every read is a single
// page and no value is ever TOASTed.
func (k Kind) InlineMax() int {
	if k == Bucket {
		return 65536
	}
	return 8120
}

// Logged reports whether the kind's tables are WAL-logged.
func (k Kind) Logged() bool { return k != Cache }

// Ref addresses a namespace by kind and name.
type Ref struct {
	Kind Kind
	Name string
}

func BucketRef(name string) Ref { return Ref{Kind: Bucket, Name: name} }
func CacheRef(name string) Ref  { return Ref{Kind: Cache, Name: name} }
func QueueRef(name string) Ref  { return Ref{Kind: Queue, Name: name} }

func (r Ref) Schema() (string, error) { return r.Kind.Schema(r.Name) }
func (r Ref) Key() string             { return string(r.Kind) + "\x00" + r.Name }
