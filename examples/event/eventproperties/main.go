package main

import (
	"github.com/kerberos-io/onvif"
	"github.com/kerberos-io/onvif/event"
	"io/ioutil"
	"log"
)

func main() {
	dev, err := onvif.NewDevice(onvif.DeviceParams{
		Xaddr: "192.168.12.148", // BOSCH
		//Xaddr:    "192.168.12.149", // Geovision
		Username: "administrator",
		Password: "Password1!",
	})
	if err != nil {
		log.Fatalln("fail to new device:", err)
	}
	// CreateUsers
	res, err := dev.CallMethod(event.GetEventProperties{})
	if err != nil {
		log.Fatalln("fail to CallMethod:", err)
	}
	bs, _ := ioutil.ReadAll(res.Body)

	log.Printf(">> Result: %+v \n %s", res.StatusCode, bs)
}
