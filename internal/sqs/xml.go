package sqs

import "encoding/xml"

const sqsNS = "http://queue.amazonaws.com/doc/2012-11-05/"

type emptyResponse struct {
	XMLName xml.Name `json:"-"`
	Xmlns   string   `xml:"xmlns,attr" json:"-"`
}

type createQueueResponse struct {
	XMLName  xml.Name `xml:"CreateQueueResponse" json:"-"`
	Xmlns    string   `xml:"xmlns,attr" json:"-"`
	QueueUrl string   `xml:"CreateQueueResult>QueueUrl" json:"QueueUrl"`
}

type getQueueURLResponse struct {
	XMLName  xml.Name `xml:"GetQueueUrlResponse" json:"-"`
	Xmlns    string   `xml:"xmlns,attr" json:"-"`
	QueueUrl string   `xml:"GetQueueUrlResult>QueueUrl" json:"QueueUrl"`
}

type listQueuesResponse struct {
	XMLName   xml.Name `xml:"ListQueuesResponse" json:"-"`
	Xmlns     string   `xml:"xmlns,attr" json:"-"`
	QueueUrls []string `xml:"ListQueuesResult>QueueUrl" json:"QueueUrls"`
}

type sendMessageResponse struct {
	XMLName          xml.Name `xml:"SendMessageResponse" json:"-"`
	Xmlns            string   `xml:"xmlns,attr" json:"-"`
	MessageId        string   `xml:"SendMessageResult>MessageId" json:"MessageId"`
	MD5OfMessageBody string   `xml:"SendMessageResult>MD5OfMessageBody" json:"MD5OfMessageBody"`
}

type sendBatchResponse struct {
	XMLName    xml.Name              `xml:"SendMessageBatchResponse" json:"-"`
	Xmlns      string                `xml:"xmlns,attr" json:"-"`
	Successful []xmlSendBatchResult  `xml:"SendMessageBatchResult>SendMessageBatchResultEntry" json:"Successful"`
	Failed     []xmlBatchResultError `xml:"SendMessageBatchResult>BatchResultErrorEntry" json:"Failed"`
}

type xmlSendBatchResult struct {
	Id               string `xml:"Id" json:"Id"`
	MessageId        string `xml:"MessageId" json:"MessageId"`
	MD5OfMessageBody string `xml:"MD5OfMessageBody" json:"MD5OfMessageBody"`
}

type xmlBatchOK struct {
	Id string `xml:"Id" json:"Id"`
}

type xmlBatchResultError struct {
	Id          string `xml:"Id" json:"Id"`
	SenderFault bool   `xml:"SenderFault" json:"SenderFault"`
	Code        string `xml:"Code" json:"Code"`
	Message     string `xml:"Message" json:"Message"`
}

type deleteBatchResponse struct {
	XMLName    xml.Name              `xml:"DeleteMessageBatchResponse" json:"-"`
	Xmlns      string                `xml:"xmlns,attr" json:"-"`
	Successful []xmlBatchOK          `xml:"DeleteMessageBatchResult>DeleteMessageBatchResultEntry" json:"Successful"`
	Failed     []xmlBatchResultError `xml:"DeleteMessageBatchResult>BatchResultErrorEntry" json:"Failed"`
}

type changeVisibilityBatchResponse struct {
	XMLName    xml.Name              `xml:"ChangeMessageVisibilityBatchResponse" json:"-"`
	Xmlns      string                `xml:"xmlns,attr" json:"-"`
	Successful []xmlBatchOK          `xml:"ChangeMessageVisibilityBatchResult>ChangeMessageVisibilityBatchResultEntry" json:"Successful"`
	Failed     []xmlBatchResultError `xml:"ChangeMessageVisibilityBatchResult>BatchResultErrorEntry" json:"Failed"`
}

type receiveMessageResponse struct {
	XMLName  xml.Name     `xml:"ReceiveMessageResponse"`
	Xmlns    string       `xml:"xmlns,attr"`
	Messages []xmlMessage `xml:"ReceiveMessageResult>Message"`
}

type xmlMessage struct {
	MessageId     string         `xml:"MessageId"`
	ReceiptHandle string         `xml:"ReceiptHandle"`
	Body          string         `xml:"Body"`
	MD5OfBody     string         `xml:"MD5OfBody"`
	Attributes    []xmlAttribute `xml:"Attribute"`
}

type xmlAttribute struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

type attributesResponse struct {
	XMLName    xml.Name       `xml:"GetQueueAttributesResponse"`
	Xmlns      string         `xml:"xmlns,attr"`
	Attributes []xmlAttribute `xml:"GetQueueAttributesResult>Attribute"`
}
