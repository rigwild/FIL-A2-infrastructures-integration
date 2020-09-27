package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func onMessageReceived(client mqtt.Client, message mqtt.Message) {
	fmt.Printf("%s <-- %s\n", message.Topic(), message.Payload())
}

func main() {
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:18830").SetClientID("listener-1")
	// opts := mqtt.NewClientOptions().AddBroker("tcp://iot.fr-par.scw.cloud:1883").SetClientID("890658db-264a-420e-9bd3-57e672b657e5")
	opts.SetCleanSession(true)

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe("fakeiot/sensors", 0, onMessageReceived); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Println("Connected to MQTT broker")
	}

	<-c
}
