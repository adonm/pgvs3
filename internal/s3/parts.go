package s3

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/adonm/pgvs3/internal/store"
)

func partsCount(m *store.Meta) int {
	if m.Parts != nil {
		return len(m.Parts)
	}
	return 1
}

func partBounds(m *store.Meta, n int) (int64, int64, bool) {
	if n < 1 || n > partsCount(m) {
		return 0, 0, false
	}
	if m.Parts == nil {
		return 0, m.Size - 1, true
	}
	var start int64
	if n > 1 {
		start = m.PartEnds[n-2]
	}
	return start, m.PartEnds[n-1] - 1, true
}

func parsePartNumber(q url.Values) (int, bool, error) {
	if !q.Has("partNumber") {
		return 0, false, nil
	}
	n, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || n < 1 || n > 10000 {
		return 0, true, errInvalidRequest("invalid partNumber")
	}
	return n, true, nil
}

func setPartsCount(w http.ResponseWriter, m *store.Meta) {
	if m.Parts != nil {
		w.Header().Set("x-amz-mp-parts-count", strconv.Itoa(len(m.Parts)))
	}
}
