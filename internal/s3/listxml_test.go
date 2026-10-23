package s3

import (
	"encoding/xml"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWriteListObjectsV2XML(t *testing.T) {
	when := time.Date(2026, 10, 23, 14, 2, 3, 456000000, time.FixedZone("x", -8*3600))
	out := listBucketResult{
		Name: "bucket", Prefix: "pre", KeyCount: 2, MaxKeys: 10,
		Delimiter: "/", IsTruncated: true,
		Contents: []listObjectXML{
			{Key: "pre/a&<b", LastModified: when, ETag: `"abc"`, Size: 7},
			{Key: "pre/c", LastModified: when, ETag: "def", Size: 8},
		},
		CommonPrefixes:        []commonPrefixXML{{Prefix: "pre/dir/"}},
		NextContinuationToken: "next-token",
		ContinuationToken:     "prev-token",
		StartAfter:            "start",
		EncodingType:          "url",
	}
	rec := httptest.NewRecorder()
	writeListObjectsV2XML(rec, 200, out)

	// Decode individual optional fields with a second struct to keep the
	// primary assertion readable.
	var doc struct {
		XMLName   xml.Name `xml:"ListBucketResult"`
		Xmlns     string   `xml:"xmlns,attr"`
		Name      string   `xml:"Name"`
		Prefix    string   `xml:"Prefix"`
		KeyCount  int      `xml:"KeyCount"`
		MaxKeys   int      `xml:"MaxKeys"`
		Delimiter string   `xml:"Delimiter"`
		Truncated bool     `xml:"IsTruncated"`
		Contents  []struct {
			Key          string `xml:"Key"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
			StorageClass string `xml:"StorageClass"`
		} `xml:"Contents"`
		CommonPrefixes []struct {
			Prefix string `xml:"Prefix"`
		} `xml:"CommonPrefixes"`
		Next       string `xml:"NextContinuationToken"`
		Prev       string `xml:"ContinuationToken"`
		StartAfter string `xml:"StartAfter"`
		Encoding   string `xml:"EncodingType"`
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, rec.Body.String())
	}
	if doc.Xmlns != s3Namespace || doc.Name != "bucket" || doc.Prefix != "pre" || doc.KeyCount != 2 || doc.MaxKeys != 10 || doc.Delimiter != "/" || !doc.Truncated {
		t.Fatalf("header mismatch: %+v", doc)
	}
	if len(doc.Contents) != 2 || doc.Contents[0].Key != "pre/a&<b" || doc.Contents[0].ETag != `"abc"` || doc.Contents[0].Size != 7 || doc.Contents[0].StorageClass != "STANDARD" {
		t.Fatalf("contents mismatch: %+v", doc.Contents)
	}
	if doc.Contents[0].LastModified != "2026-10-23T22:02:03.456Z" {
		t.Fatalf("time = %q", doc.Contents[0].LastModified)
	}
	if len(doc.CommonPrefixes) != 1 || doc.CommonPrefixes[0].Prefix != "pre/dir/" {
		t.Fatalf("common prefixes = %+v", doc.CommonPrefixes)
	}
	if doc.Next != "next-token" || doc.Prev != "prev-token" || doc.StartAfter != "start" || doc.Encoding != "url" {
		t.Fatalf("tokens = %+v", doc)
	}
}
