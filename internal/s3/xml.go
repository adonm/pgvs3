package s3

import (
	"bytes"
	"encoding/xml"
	"net/http"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

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

type deleteRequest struct {
	Quiet   bool `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

func quoteETag(etag string) string {
	return `"` + etag + `"`
}

func writeXML(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	if err := xml.NewEncoder(&buf).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}
