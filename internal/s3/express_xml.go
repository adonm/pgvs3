package s3

import "encoding/xml"

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
	XMLName            xml.Name    `xml:"ListMultipartUploadsResult"`
	Xmlns              string      `xml:"xmlns,attr"`
	Bucket             string      `xml:"Bucket"`
	KeyMarker          string      `xml:"KeyMarker"`
	UploadIDMarker     string      `xml:"UploadIdMarker"`
	NextKeyMarker      string      `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string      `xml:"NextUploadIdMarker,omitempty"`
	Delimiter          string      `xml:"Delimiter,omitempty"`
	Prefix             string      `xml:"Prefix"`
	MaxUploads         int         `xml:"MaxUploads"`
	IsTruncated        bool        `xml:"IsTruncated"`
	Uploads            []uploadXML `xml:"Upload"`
}
