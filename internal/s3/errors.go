package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
)

// APIError is an S3 error response.
type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func apiErr(code, message string, status int) *APIError {
	return &APIError{Code: code, Message: message, Status: status}
}

func errAccessDenied(msg string) *APIError {
	return apiErr("AccessDenied", msg, http.StatusForbidden)
}

func errNoSuchKey() *APIError {
	return apiErr("NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}

func errNoSuchBucket() *APIError {
	return apiErr("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}

func errNoSuchUpload() *APIError {
	return apiErr("NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound)
}

func errPreconditionFailed() *APIError {
	return apiErr("PreconditionFailed", "At least one of the preconditions you specified did not hold.", http.StatusPreconditionFailed)
}

func errInvalidRange() *APIError {
	return apiErr("InvalidRange", "The requested range is not satisfiable.", http.StatusRequestedRangeNotSatisfiable)
}

func errInvalidPart() *APIError {
	return apiErr("InvalidPart", "One or more of the specified parts could not be found or did not match the supplied ETag.", http.StatusBadRequest)
}

func errInvalidRequest(msg string) *APIError {
	return apiErr("InvalidRequest", msg, http.StatusBadRequest)
}

func errNotImplemented() *APIError {
	return apiErr("NotImplemented", "A header or operation you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func errInternal(err error, log *slog.Logger) *APIError {
	log.Error("internal error", "err", err)
	return apiErr("InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError)
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

func writeError(w http.ResponseWriter, r *http.Request, err error, log *slog.Logger) {
	var ae *APIError
	if !errors.As(err, &ae) {
		ae = errInternal(err, log)
	}
	w.Header().Set("x-amz-request-id", requestID())
	if r.Method == http.MethodHead {
		w.WriteHeader(ae.Status)
		return
	}
	body, _ := xml.Marshal(errorResponse{
		Code: ae.Code, Message: ae.Message, Resource: r.URL.Path, RequestID: requestID(),
	})
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(ae.Status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

var reqSeq atomic.Uint64

func requestID() string {
	return fmt.Sprintf("%016x", reqSeq.Add(1))
}

func requestIDHeader() string { return requestID() }

var _ = fmt.Sprintf
