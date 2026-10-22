package s3

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/adonm/pgvs3/internal/store"
)

func (s *Server) appendObject(w http.ResponseWriter, r *http.Request, bucket, key, offsetHeader string) {
	offset, err := strconv.ParseInt(offsetHeader, 10, 64)
	if err != nil || offset < 0 {
		writeError(w, r, errInvalidRequest("invalid x-amz-write-offset-bytes"), s.log)
		return
	}
	if err := validateSSE(r); err != nil {
		writeError(w, r, err, s.log)
		return
	}
	cond, ifMatch, err := putCondition(r)
	if err != nil {
		writeError(w, r, err, s.log)
		return
	}
	in, err := newIntegrity(r.Header)
	if err != nil {
		writeError(w, r, err, s.log)
		return
	}
	writer, err := s.store.StartAppend(r.Context(), bucket, key)
	if err != nil {
		s.metaError(w, r, bucket, err)
		return
	}
	body := io.ReadCloser(r.Body)
	if len(in.h) > 0 {
		body = &integrityReader{r: body, in: in}
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(writer, body, buf); err != nil {
		writer.Abort()
		writeError(w, r, bodyError(err), s.log)
		return
	}
	checks, err := in.verify(r.Trailer)
	if err != nil {
		writer.Abort()
		writeError(w, r, err, s.log)
		return
	}
	pub, err := writer.Finish(checks)
	if err != nil {
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	meta, err := s.store.FinishAppend(r.Context(), bucket, key, offset, pub.FileID, pub.Size, pub.SHA256, cond, ifMatch)
	if err != nil {
		s.store.DiscardFile(r.Context(), pub.FileID)
		switch {
		case errors.Is(err, store.ErrInvalidOffset):
			writeError(w, r, apiErr("InvalidWriteOffset", "The write offset does not match the object size.", http.StatusConflict), s.log)
		case errors.Is(err, store.ErrTooManyParts):
			writeError(w, r, apiErr("TooManyParts", "The object has reached the 10,000 part limit.", http.StatusBadRequest), s.log)
		case errors.Is(err, store.ErrInlineTooLarge):
			writeError(w, r, errInvalidRequest("appending would exceed the 64 KiB inline object limit"), s.log)
		case errors.Is(err, store.ErrPrecondition):
			writeError(w, r, errPreconditionFailed(), s.log)
		case errors.Is(err, store.ErrNoBucket):
			writeError(w, r, errNoSuchBucket(), s.log)
		default:
			writeError(w, r, errInternal(err, s.log), s.log)
		}
		return
	}
	w.Header().Set("ETag", quoteETag(meta.ETag))
	w.Header().Set("x-amz-object-size", strconv.FormatInt(meta.Size, 10))
	s.setDirectoryObjectHeaders(w, bucket)
	w.WriteHeader(http.StatusOK)
}
