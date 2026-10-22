package sqs

import "encoding/xml"

const sqsNS = "http://queue.amazonaws.com/doc/2012-11-05/"

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
