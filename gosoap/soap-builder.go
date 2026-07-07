package gosoap

import (
	"encoding/xml"
	"errors"
	"log"

	"github.com/beevik/etree"
)

//SoapMessage type from string
type SoapMessage string

// NewEmptySOAP return new SoapMessage
func NewEmptySOAP() SoapMessage {
	doc := buildSoapRoot()
	//doc.IndentTabs()

	res, _ := doc.WriteToString()

	return SoapMessage(res)
}

//NewSOAP Get a new soap message
func NewSOAP(headContent []*etree.Element, bodyContent []*etree.Element, namespaces map[string]string) SoapMessage {
	doc := buildSoapRoot()
	//doc.IndentTabs()

	res, _ := doc.WriteToString()

	return SoapMessage(res)
}

func (msg SoapMessage) String() string {
	return string(msg)
}

//StringIndent handle indent
func (msg SoapMessage) StringIndent() string {
	doc := etree.NewDocument()

	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}

	doc.IndentTabs()
	res, _ := doc.WriteToString()

	return res
}

//Body return body from Envelope
func (msg SoapMessage) Body() string {

	doc := etree.NewDocument()

	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}
	bodyTag := doc.Root().SelectElement("Body").ChildElements()[0]
	doc.SetRoot(bodyTag)
	doc.IndentTabs()

	res, _ := doc.WriteToString()

	return res
}

//AddStringBodyContent for Envelope
func (msg *SoapMessage) AddStringBodyContent(data string) {
	doc := etree.NewDocument()

	if err := doc.ReadFromString(data); err != nil {
		log.Println(err.Error())
	}

	element := doc.Root()

	doc = etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}
	//doc.FindElement("./Envelope/Body").AddChild(element)
	bodyTag := doc.Root().SelectElement("Body")
	if element != nil {
		bodyTag.AddChild(element)
	}

	//doc.IndentTabs()
	res, _ := doc.WriteToString()

	*msg = SoapMessage(res)
}

//AddBodyContent for Envelope
func (msg *SoapMessage) AddBodyContent(element *etree.Element) {
	doc := etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}
	//doc.FindElement("./Envelope/Body").AddChild(element)
	bodyTag := doc.Root().SelectElement("Body")
	bodyTag.AddChild(element)

	//doc.IndentTabs()
	res, _ := doc.WriteToString()

	*msg = SoapMessage(res)
}

//AddBodyContents for Envelope body
func (msg *SoapMessage) AddBodyContents(elements []*etree.Element) {
	doc := etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}

	bodyTag := doc.Root().SelectElement("Body")

	if len(elements) != 0 {
		for _, j := range elements {
			bodyTag.AddChild(j)
		}
	}

	//doc.IndentTabs()
	res, _ := doc.WriteToString()

	*msg = SoapMessage(res)
}

// AddStringHeaderContent appends a single root element to the SOAP
// Header. Use AddStringHeaderContents (plural) when the content
// contains multiple sibling elements — for example WS-Addressing
// reference parameters, which the spec requires as separate header
// blocks. The two coexist for backwards compatibility: external
// consumers of this library may rely on AddStringHeaderContent's
// single-root constraint and the matching error on multi-root input.
func (msg *SoapMessage) AddStringHeaderContent(data string) error {
	doc := etree.NewDocument()

	if err := doc.ReadFromString(data); err != nil {
		//log.Println(err.Error())
		return err
	}

	element := doc.Root()

	doc = etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		//log.Println(err.Error())
		return err
	}

	bodyTag := doc.Root().SelectElement("Header")
	bodyTag.AddChild(element)

	//doc.IndentTabs()
	res, _ := doc.WriteToString()

	*msg = SoapMessage(res)

	return nil
}

// AddStringHeaderContents is the multi-root variant of
// AddStringHeaderContent: it accepts any number of top-level sibling
// elements (zero is an error) and appends each as its own SOAP Header
// child. Needed because WS-Addressing 1.0 SOAP Binding §3.4 requires each
// reference parameter to be a separate Header block, but a Go XML
// document only has one root. Comments and text outside elements are
// silently dropped.
//
// SECURITY: data forwards verbatim into the outbound envelope. Do not
// pass content sourced from untrusted clients — see the same caveat
// on onvif.Device.SendSoapWithOptions / SendSoapWithHeader.
func (msg *SoapMessage) AddStringHeaderContents(data string) error {
	in := etree.NewDocument()
	if err := in.ReadFromString("<wrap>" + data + "</wrap>"); err != nil {
		return err
	}
	wrap := in.SelectElement("wrap")
	if wrap == nil {
		return errors.New("AddStringHeaderContents: missing wrap root")
	}
	children := wrap.ChildElements()
	if len(children) == 0 {
		return errors.New("AddStringHeaderContents: no element children in content")
	}

	doc := etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		return err
	}
	header := doc.Root().SelectElement("Header")
	for _, child := range children {
		header.AddChild(child.Copy())
	}
	res, _ := doc.WriteToString()
	*msg = SoapMessage(res)
	return nil
}

//AddHeaderContent for Envelope body
func (msg *SoapMessage) AddHeaderContent(element *etree.Element) {
	doc := etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}

	bodyTag := doc.Root().SelectElement("Header")
	bodyTag.AddChild(element)

	//doc.IndentTabs()
	res, _ := doc.WriteToString()

	*msg = SoapMessage(res)
}

//AddHeaderContents for Envelope body
func (msg *SoapMessage) AddHeaderContents(elements []*etree.Element) {
	doc := etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}

	headerTag := doc.Root().SelectElement("Header")

	if len(elements) != 0 {
		for _, j := range elements {
			headerTag.AddChild(j)
		}
	}

	//doc.IndentTabs()
	res, _ := doc.WriteToString()

	*msg = SoapMessage(res)
}

//AddRootNamespace for Envelope body
func (msg *SoapMessage) AddRootNamespace(key, value string) {
	doc := etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}
	doc.Root().CreateAttr("xmlns:"+key, value)
	//doc.IndentTabs()
	res, _ := doc.WriteToString()

	*msg = SoapMessage(res)
}

//AddRootNamespaces for Envelope body
func (msg *SoapMessage) AddRootNamespaces(namespaces map[string]string) {
	for key, value := range namespaces {
		msg.AddRootNamespace(key, value)
	}

	/*
		doc := etree.NewDocument()
		if err := doc.ReadFromString(msg.String()); err != nil {
			//log.Println(err.Error())
			return err
		}

		for key, value := range namespaces {
			doc.Root().CreateAttr("xmlns:" + key, value)
		}

		doc.IndentTabs()
		res, _ := doc.WriteToString()

		*msg = SoapMessage(res)*/
}

func buildSoapRoot() *etree.Document {
	doc := etree.NewDocument()

	doc.CreateProcInst("xml", `version="1.0" encoding="UTF-8"`)

	env := doc.CreateElement("soap-env:Envelope")
	env.CreateElement("soap-env:Header")
	env.CreateElement("soap-env:Body")

	env.CreateAttr("xmlns:soap-env", "http://www.w3.org/2003/05/soap-envelope")
	env.CreateAttr("xmlns:soap-enc", "http://www.w3.org/2003/05/soap-encoding")

	return doc
}

//AddWSSecurity Header for soapMessage
func (msg *SoapMessage) AddWSSecurity(username, password string) {
	//doc := etree.NewDocument()
	//if err := doc.ReadFromString(msg.String()); err != nil {
	//	log.Println(err.Error())
	//}
	/*
		Getting an WS-Security struct representation
	*/
	auth := NewSecurity(username, password)

	/*
		Adding WS-Security namespaces to root element of SOAP message
	*/
	//msg.AddRootNamespace("wsse", "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext1.0.xsd")
	//msg.AddRootNamespace("wsu", "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility1.0.xsd")

	soapReq, err := xml.MarshalIndent(auth, "", "  ")
	if err != nil {
		//log.Printf("error: %v\n", err.Error())
		panic(err)
	}

	/*
		Adding WS-Security struct to SOAP header
	*/
	msg.AddStringHeaderContent(string(soapReq))

	//doc.IndentTabs()
	//res, _ := doc.WriteToString()
	//
	//*msg = SoapMessage(res)
}

//AddAction Header handling for soapMessage
func (msg *SoapMessage) AddAction() {

	doc := etree.NewDocument()
	if err := doc.ReadFromString(msg.String()); err != nil {
		log.Println(err.Error())
	}
	// opetaionTag := doc.Root().SelectElement("Body")

	// firstElemnt := opetaionTag.Child[0]

	// soapReq, err := xml.MarshalIndent(action, "", "  ")
	// if err != nil {
	// 	//log.Printf("error: %v\n", err.Error())
	// 	panic(err)
	// }
	// /*
	// 	Adding WS-Security struct to SOAP header
	// */
	// msg.AddStringHeaderContent(string(soapReq))

	// //doc.IndentTabs()
	// //res, _ := doc.WriteToString()
	// //
	// //*msg = SoapMessage(res)
}
