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

func isoTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
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
	w.Header().Set("x-amz-request-id", requestID())
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

var listXMLWriterPool = sync.Pool{New: func() any {
	return bufio.NewWriterSize(io.Discard, 32<<10)
}}

// writeListObjectsV2XML writes the hot listing response without reflection.
// Listings can contain 1,000 objects; encoding/xml spends most of its time in
// reflection and repeated string allocation on this path.
func writeListObjectsV2XML(w http.ResponseWriter, status int, out listBucketResult) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", requestID())
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
	t = t.UTC()
	y, month, day := t.Date()
	hour, minute, second := t.Clock()
	ms := t.Nanosecond() / int(time.Millisecond)
	dst = append4(dst, y)
	dst = append(dst, '-')
	dst = append2(dst, int(month))
	dst = append(dst, '-')
	dst = append2(dst, day)
	dst = append(dst, 'T')
	dst = append2(dst, hour)
	dst = append(dst, ':')
	dst = append2(dst, minute)
	dst = append(dst, ':')
	dst = append2(dst, second)
	dst = append(dst, '.')
	dst = append3(dst, ms)
	return append(dst, 'Z')
}

func append2(dst []byte, n int) []byte {
	return append(dst, byte('0'+n/10%10), byte('0'+n%10))
}

func append3(dst []byte, n int) []byte {
	return append(dst, byte('0'+n/100%10), byte('0'+n/10%10), byte('0'+n%10))
}

func append4(dst []byte, n int) []byte {
	return append(dst, byte('0'+n/1000%10), byte('0'+n/100%10), byte('0'+n/10%10), byte('0'+n%10))
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
