package s3

import (
	"bufio"
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type checksumXML struct {
	CRC32     string `xml:"ChecksumCRC32,omitempty"`
	CRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	CRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	SHA1      string `xml:"ChecksumSHA1,omitempty"`
	SHA256    string `xml:"ChecksumSHA256,omitempty"`
	SHA512    string `xml:"ChecksumSHA512,omitempty"`
	Type      string `xml:"ChecksumType,omitempty"`
}

type partXML struct {
	PartNumber        int    `xml:"PartNumber"`
	LastModified      string `xml:"LastModified,omitempty"`
	ETag              string `xml:"ETag,omitempty"`
	Size              int64  `xml:"Size"`
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

type objectPartsXML struct {
	IsTruncated          bool      `xml:"IsTruncated"`
	MaxParts             int       `xml:"MaxParts"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker,omitempty"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	PartsCount           int       `xml:"PartsCount"`
	Parts                []partXML `xml:"Part"`
}

type getObjectAttributesResponse struct {
	XMLName      xml.Name        `xml:"GetObjectAttributesResponse"`
	Xmlns        string          `xml:"xmlns,attr"`
	ETag         string          `xml:"ETag,omitempty"`
	Checksum     *checksumXML    `xml:"Checksum,omitempty"`
	ObjectParts  *objectPartsXML `xml:"ObjectParts,omitempty"`
	StorageClass string          `xml:"StorageClass,omitempty"`
	ObjectSize   *int64          `xml:"ObjectSize,omitempty"`
}

type listPartsResult struct {
	XMLName              xml.Name  `xml:"ListPartsResult"`
	Xmlns                string    `xml:"xmlns,attr"`
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
	StorageClass         string    `xml:"StorageClass"`
	ChecksumAlgorithm    string    `xml:"ChecksumAlgorithm,omitempty"`
	Initiator            ownerXML  `xml:"Initiator"`
	Owner                ownerXML  `xml:"Owner"`
	Parts                []partXML `xml:"Part"`
}

type uploadXML struct {
	Key          string   `xml:"Key"`
	UploadID     string   `xml:"UploadId"`
	Initiator    ownerXML `xml:"Initiator"`
	Owner        ownerXML `xml:"Owner"`
	StorageClass string   `xml:"StorageClass"`
	Initiated    string   `xml:"Initiated"`
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	Xmlns              string            `xml:"xmlns,attr"`
	Bucket             string            `xml:"Bucket"`
	KeyMarker          string            `xml:"KeyMarker"`
	UploadIDMarker     string            `xml:"UploadIdMarker"`
	NextKeyMarker      string            `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string            `xml:"NextUploadIdMarker,omitempty"`
	Delimiter          string            `xml:"Delimiter,omitempty"`
	Prefix             string            `xml:"Prefix"`
	MaxUploads         int               `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
	Uploads            []uploadXML       `xml:"Upload"`
	CommonPrefixes     []commonPrefixXML `xml:"CommonPrefixes"`
}

type listObjectXML struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
}

type commonPrefixXML struct {
	Prefix string `xml:"Prefix"`
}

type listBucketResult struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Xmlns                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []listObjectXML   `xml:"Contents"`
	CommonPrefixes        []commonPrefixXML `xml:"CommonPrefixes"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
}

type listVersionsResult struct {
	XMLName             xml.Name     `xml:"ListVersionsResult"`
	Xmlns               string       `xml:"xmlns,attr"`
	Name                string       `xml:"Name"`
	Prefix              string       `xml:"Prefix"`
	KeyMarker           string       `xml:"KeyMarker"`
	VersionIdMarker     string       `xml:"VersionIdMarker"`
	NextKeyMarker       string       `xml:"NextKeyMarker,omitempty"`
	NextVersionIdMarker string       `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int          `xml:"MaxKeys"`
	IsTruncated         bool         `xml:"IsTruncated"`
	Versions            []versionXML `xml:"Version"`
	EncodingType        string       `xml:"EncodingType,omitempty"`
}

type versionXML struct {
	Key          string   `xml:"Key"`
	VersionId    string   `xml:"VersionId"`
	IsLatest     bool     `xml:"IsLatest"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
	Size         int64    `xml:"Size"`
	StorageClass string   `xml:"StorageClass"`
	Owner        ownerXML `xml:"Owner"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Xmlns   string      `xml:"xmlns,attr"`
	Owner   ownerXML    `xml:"Owner"`
	Buckets []bucketXML `xml:"Buckets>Bucket"`
}

type ownerXML struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type locationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

type deleteResult struct {
	XMLName xml.Name       `xml:"DeleteResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Deleted []deletedXML   `xml:"Deleted"`
	Errors  []deleteErrXML `xml:"Error"`
}

type deletedXML struct {
	Key string `xml:"Key"`
}

type deleteErrXML struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type completePartXML struct {
	PartNumber        int    `xml:"PartNumber"`
	ETag              string `xml:"ETag"`
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name          `xml:"CompleteMultipartUpload"`
	Parts   []completePartXML `xml:"Part"`
}

const isoTimeLayout = "2006-01-02T15:04:05.000Z"

func isoTime(t time.Time) string {
	return t.UTC().Format(isoTimeLayout)
}

func quoteETag(etag string) string {
	return `"` + strings.Trim(etag, `"`) + `"`
}

func writeXML(w http.ResponseWriter, status int, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	ensureRequestID(w)
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

var listXMLWriterPool = sync.Pool{New: func() any {
	return bufio.NewWriterSize(io.Discard, 32<<10)
}}

func writeListObjectsV2XML(w http.ResponseWriter, status int, out listBucketResult) {
	w.Header().Set("Content-Type", "application/xml")
	ensureRequestID(w)
	w.WriteHeader(status)

	bw := listXMLWriterPool.Get().(*bufio.Writer)
	bw.Reset(w)
	defer func() {
		bw.Reset(io.Discard)
		listXMLWriterPool.Put(bw)
	}()

	_, _ = bw.WriteString(xml.Header)
	_, _ = bw.WriteString(`<ListBucketResult xmlns="`)
	_, _ = bw.WriteString(s3Namespace)
	_, _ = bw.WriteString(`">`)
	writeXMLStringElement(bw, "Name", out.Name)
	writeXMLStringElement(bw, "Prefix", out.Prefix)
	writeXMLIntElement(bw, "KeyCount", int64(out.KeyCount))
	writeXMLIntElement(bw, "MaxKeys", int64(out.MaxKeys))
	if out.Delimiter != "" {
		writeXMLStringElement(bw, "Delimiter", out.Delimiter)
	}
	writeXMLBoolElement(bw, "IsTruncated", out.IsTruncated)
	for _, obj := range out.Contents {
		_, _ = bw.WriteString("<Contents>")
		writeXMLStringElement(bw, "Key", obj.Key)
		writeXMLTimeElement(bw, "LastModified", obj.LastModified)
		writeXMLETagElement(bw, obj.ETag)
		writeXMLIntElement(bw, "Size", obj.Size)
		_, _ = bw.WriteString("<StorageClass>STANDARD</StorageClass></Contents>")
	}
	for _, cp := range out.CommonPrefixes {
		_, _ = bw.WriteString("<CommonPrefixes>")
		writeXMLStringElement(bw, "Prefix", cp.Prefix)
		_, _ = bw.WriteString("</CommonPrefixes>")
	}
	if out.NextContinuationToken != "" {
		writeXMLStringElement(bw, "NextContinuationToken", out.NextContinuationToken)
	}
	if out.ContinuationToken != "" {
		writeXMLStringElement(bw, "ContinuationToken", out.ContinuationToken)
	}
	if out.StartAfter != "" {
		writeXMLStringElement(bw, "StartAfter", out.StartAfter)
	}
	if out.EncodingType != "" {
		writeXMLStringElement(bw, "EncodingType", out.EncodingType)
	}
	_, _ = bw.WriteString("</ListBucketResult>")
	_ = bw.Flush()
}

func writeXMLStringElement(bw *bufio.Writer, tag, value string) {
	_ = bw.WriteByte('<')
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
	writeXMLText(bw, value)
	_, _ = bw.WriteString("</")
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
}

func writeXMLIntElement(bw *bufio.Writer, tag string, n int64) {
	var buf [24]byte
	_ = bw.WriteByte('<')
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
	_, _ = bw.Write(strconv.AppendInt(buf[:0], n, 10))
	_, _ = bw.WriteString("</")
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
}

func writeXMLBoolElement(bw *bufio.Writer, tag string, value bool) {
	_ = bw.WriteByte('<')
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
	if value {
		_, _ = bw.WriteString("true")
	} else {
		_, _ = bw.WriteString("false")
	}
	_, _ = bw.WriteString("</")
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
}

func writeXMLTimeElement(bw *bufio.Writer, tag string, t time.Time) {
	var buf [24]byte
	_ = bw.WriteByte('<')
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
	_, _ = bw.Write(appendISOTime(buf[:0], t))
	_, _ = bw.WriteString("</")
	_, _ = bw.WriteString(tag)
	_ = bw.WriteByte('>')
}

func appendISOTime(dst []byte, t time.Time) []byte {
	return t.UTC().AppendFormat(dst, isoTimeLayout)
}

func writeXMLETagElement(bw *bufio.Writer, etag string) {
	_, _ = bw.WriteString("<ETag>\"")
	writeXMLText(bw, strings.Trim(etag, `"`))
	_, _ = bw.WriteString("\"</ETag>")
}

func writeXMLText(bw *bufio.Writer, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x7f || c == '<' || c == '>' || c == '&' || c == '"' || c == '\'' {
			_ = xml.EscapeText(bw, []byte(s))
			return
		}
	}
	_, _ = bw.WriteString(s)
}
