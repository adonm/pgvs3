package s3

type deleteRequest struct {
	Quiet   bool `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

func quoteETag(etag string) string { return `"` + etag + `"` }
