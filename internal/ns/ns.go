package ns

import "errors"

type Kind string

const (
	Bucket    Kind = "bucket"
	Cache     Kind = "cache"
	WriteBack Kind = "writeback"
	Queue     Kind = "queue"
)

var ErrInvalidName = errors.New("invalid namespace name")

var ErrUnknownKind = errors.New("unknown namespace kind")

func (k Kind) Valid() bool {
	switch k {
	case Bucket, Cache, WriteBack, Queue:
		return true
	}
	return false
}

func (k Kind) Prefix() string {
	switch k {
	case Cache:
		return "s3c_"
	case WriteBack:
		return "s3w_"
	case Queue:
		return "s3q_"
	default:
		return "s3b_"
	}
}

const MaxNameLen = 58

func (k Kind) Schema(name string) (string, error) {
	if !k.Valid() {
		return "", ErrUnknownKind
	}
	if !ValidName(name) {
		return "", ErrInvalidName
	}
	return k.Prefix() + name, nil
}

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

func ParseSchema(schema string) (Kind, string, bool) {
	var k Kind
	switch {
	case len(schema) > 4 && schema[:4] == "s3b_":
		k = Bucket
	case len(schema) > 4 && schema[:4] == "s3c_":
		k = Cache
	case len(schema) > 4 && schema[:4] == "s3w_":
		k = WriteBack
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

func (k Kind) InlineMax() int {
	if k == Bucket || k == WriteBack {
		return 65536
	}
	return 8120
}

func (k Kind) MaxKeyLen() int {
	if k == Bucket {
		return 1024
	}
	return 2048
}

func (k Kind) Logged() bool { return true }

type Ref struct {
	Kind Kind
	Name string
}

func BucketRef(name string) Ref    { return Ref{Kind: Bucket, Name: name} }
func CacheRef(name string) Ref     { return Ref{Kind: Cache, Name: name} }
func WriteBackRef(name string) Ref { return Ref{Kind: WriteBack, Name: name} }
func QueueRef(name string) Ref     { return Ref{Kind: Queue, Name: name} }

func (r Ref) Schema() (string, error) { return r.Kind.Schema(r.Name) }
func (r Ref) Key() string             { return string(r.Kind) + "\x00" + r.Name }
