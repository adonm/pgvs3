package sqs

import "encoding/xml"

const sqsNS = "http://queue.amazonaws.com/doc/2012-11-05/"

type emptyResponse struct {
	XMLName xml.Name
	Xmlns   string `xml:"xmlns,attr"`
}

type createQueueResponse struct {
	XMLName  xml.Name `xml:"CreateQueueResponse"`
	Xmlns    string   `xml:"xmlns,attr"`
	QueueUrl string   `xml:"CreateQueueResult>QueueUrl"`
}

type getQueueURLResponse struct {
	XMLName  xml.Name `xml:"GetQueueUrlResponse"`
	Xmlns    string   `xml:"xmlns,attr"`
	QueueUrl string   `xml:"GetQueueUrlResult>QueueUrl"`
}

type listQueuesResponse struct {
	XMLName   xml.Name `xml:"ListQueuesResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	QueueUrls []string `xml:"ListQueuesResult>QueueUrl"`
}

type sendMessageResponse struct {
	XMLName          xml.Name `xml:"SendMessageResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	MessageId        string   `xml:"SendMessageResult>MessageId"`
	MD5OfMessageBody string   `xml:"SendMessageResult>MD5OfMessageBody"`
}

type sendBatchResponse struct {
	XMLName    xml.Name              `xml:"SendMessageBatchResponse"`
	Xmlns      string                `xml:"xmlns,attr"`
	Successful []xmlSendBatchResult  `xml:"SendMessageBatchResult>SendMessageBatchResultEntry"`
	Failed     []xmlBatchResultError `xml:"SendMessageBatchResult>BatchResultErrorEntry"`
}

type xmlSendBatchResult struct {
	Id               string `xml:"Id"`
	MessageId        string `xml:"MessageId"`
	MD5OfMessageBody string `xml:"MD5OfMessageBody"`
}

type xmlBatchOK struct {
	Id string `xml:"Id"`
}

type xmlBatchResultError struct {
	Id          string `xml:"Id"`
	SenderFault bool   `xml:"SenderFault"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
}

type deleteBatchResponse struct {
	XMLName    xml.Name              `xml:"DeleteMessageBatchResponse"`
	Xmlns      string                `xml:"xmlns,attr"`
	Successful []xmlBatchOK          `xml:"DeleteMessageBatchResult>DeleteMessageBatchResultEntry"`
	Failed     []xmlBatchResultError `xml:"DeleteMessageBatchResult>BatchResultErrorEntry"`
}

type changeVisibilityBatchResponse struct {
	XMLName    xml.Name              `xml:"ChangeMessageVisibilityBatchResponse"`
	Xmlns      string                `xml:"xmlns,attr"`
	Successful []xmlBatchOK          `xml:"ChangeMessageVisibilityBatchResult>ChangeMessageVisibilityBatchResultEntry"`
	Failed     []xmlBatchResultError `xml:"ChangeMessageVisibilityBatchResult>BatchResultErrorEntry"`
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
