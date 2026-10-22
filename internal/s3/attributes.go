package s3

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/adonm/pgvs3/internal/store"
)

func (s *Server) getObjectAttributes(w http.ResponseWriter, r *http.Request, bucket, key string) {
	requested := map[string]bool{}
	for _, a := range strings.Split(r.Header.Get("x-amz-object-attributes"), ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			requested[a] = true
		}
	}
	if len(requested) == 0 {
		writeError(w, r, errInvalidRequest("x-amz-object-attributes is required"), s.log)
		return
	}
	meta, parts, err := s.store.Attributes(r.Context(), bucket, key)
	if err != nil {
		s.metaError(w, r, bucket, err)
		return
	}
	maxParts := 1000
	if v := r.Header.Get("x-amz-max-parts"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidRequest("invalid x-amz-max-parts"), s.log)
			return
		}
		maxParts = min(max(n, 1), 1000)
	}
	marker := 0
	if v := r.Header.Get("x-amz-part-number-marker"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidRequest("invalid x-amz-part-number-marker"), s.log)
			return
		}
		marker = n
	}
	out := getObjectAttributesResponse{Xmlns: s3Namespace}
	if requested["ETag"] {
		out.ETag = quoteETag(meta.ETag)
	}
	if requested["ObjectSize"] {
		size := meta.Size
		out.ObjectSize = &size
	}
	if requested["StorageClass"] {
		out.StorageClass = "STANDARD"
	}
	if requested["Checksum"] {
		out.Checksum = &checksumXML{
			SHA256: base64.StdEncoding.EncodeToString(meta.SHA256),
			Type:   "FULL_OBJECT",
		}
		if meta.Parts != nil {
			out.Checksum.Type = "COMPOSITE"
		}
	}
	if requested["ObjectParts"] && meta.Parts != nil {
		op := &objectPartsXML{MaxParts: maxParts, PartNumberMarker: marker, PartsCount: len(parts)}
		for _, p := range parts {
			if int(p.Number) <= marker {
				continue
			}
			if len(op.Parts) >= maxParts {
				op.IsTruncated = true
				op.NextPartNumberMarker = int(op.Parts[len(op.Parts)-1].PartNumber)
				break
			}
			part := partXML{PartNumber: int(p.Number), Size: p.Size}
			setPartChecksum(&part, p.Checksum)
			op.Parts = append(op.Parts, part)
		}
		out.ObjectParts = op
	}
	w.Header().Set("Last-Modified", meta.CreatedAt.UTC().Format(http.TimeFormat))
	writeXML(w, http.StatusOK, out)
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
	uploadID := q.Get("uploadId")
	max := 1000
	if v := q.Get("max-parts"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidRequest("invalid max-parts"), s.log)
			return
		}
		max = min(n, 1000)
	}
	marker := int32(0)
	if v := q.Get("part-number-marker"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidRequest("invalid part-number-marker"), s.log)
			return
		}
		marker = int32(n)
	}
	parts, truncated, next, err := s.store.ListParts(r.Context(), uploadID, bucket, key, max, marker)
	if err != nil {
		if errors.Is(err, store.ErrNoUpload) {
			writeError(w, r, errNoSuchUpload(), s.log)
			return
		}
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	storageClass := "STANDARD"
	out := listPartsResult{
		Xmlns: s3Namespace, Bucket: bucket, Key: key, UploadID: uploadID,
		PartNumberMarker: int(marker), MaxParts: max, IsTruncated: truncated,
		StorageClass: storageClass,
		Initiator:    ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
		Owner:        ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
	}
	if truncated {
		out.NextPartNumberMarker = int(next)
	}
	for _, p := range parts {
		part := partXML{
			PartNumber: int(p.Number), Size: p.Size, LastModified: isoTime(p.LastModified),
			ETag: quoteETag(p.ETag),
		}
		setPartChecksum(&part, p.Checksum)
		out.Parts = append(out.Parts, part)
	}
	writeXML(w, http.StatusOK, out)
}

func setPartChecksum(part *partXML, c *store.Checksum) {
	if c == nil {
		return
	}
	switch c.Name {
	case "CRC32":
		part.ChecksumCRC32 = c.Value
	case "CRC32C":
		part.ChecksumCRC32C = c.Value
	case "CRC64NVME":
		part.ChecksumCRC64NVME = c.Value
	case "SHA1":
		part.ChecksumSHA1 = c.Value
	case "SHA256":
		part.ChecksumSHA256 = c.Value
	}
}

func (s *Server) listMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) {
	prefix := q.Get("prefix")
	max := 1000
	if v := q.Get("max-uploads"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidRequest("invalid max-uploads"), s.log)
			return
		}
		max = min(n, 1000)
	}
	keyMarker := q.Get("key-marker")
	uploadMarker := q.Get("upload-id-marker")
	uploads, truncated, err := s.store.ListUploads(r.Context(), bucket, prefix, prefixEnd(prefix), keyMarker, uploadMarker, max)
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	storageClass := "STANDARD"
	delimiter := q.Get("delimiter")
	out := listMultipartUploadsResult{
		Xmlns: s3Namespace, Bucket: bucket, Prefix: prefix, KeyMarker: keyMarker,
		UploadIDMarker: uploadMarker, MaxUploads: max, IsTruncated: truncated,
		Delimiter: delimiter,
	}
	seen := map[string]bool{}
	if delimiter != "" {
		if rest := strings.TrimPrefix(keyMarker, prefix); rest != keyMarker {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				seen[prefix+rest[:idx+len(delimiter)]] = true
			}
		}
	}
	lastKey, lastID := keyMarker, uploadMarker
	for _, u := range uploads {
		lastKey, lastID = u.Key, u.UploadID
		if delimiter != "" {
			rest := strings.TrimPrefix(u.Key, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seen[cp] {
					if len(out.Uploads)+len(out.CommonPrefixes) >= max {
						out.IsTruncated = true
						break
					}
					seen[cp] = true
					out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: cp})
				}
				continue
			}
		}
		if len(out.Uploads)+len(out.CommonPrefixes) >= max {
			out.IsTruncated = true
			break
		}
		out.Uploads = append(out.Uploads, uploadXML{
			Key: u.Key, UploadID: u.UploadID, StorageClass: storageClass, Initiated: isoTime(u.Initiated),
			Initiator: ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
			Owner:     ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
		})
	}
	if out.IsTruncated {
		out.NextKeyMarker, out.NextUploadIDMarker = lastKey, lastID
	}
	writeXML(w, http.StatusOK, out)
}
