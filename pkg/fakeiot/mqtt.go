package fakeiot

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/viper"
)

// newClient will start a new mqtt client
func newClient(clientID string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(viper.GetString("mqtt.endpoint"))
	opts.SetClientID(clientID)
	opts.SetCleanSession(true)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	return client
}

// mqttConnect will start all mqtt clients
func mqttConnect() []mqtt.Client {
	t := viper.GetStringSlice("fakeiot.mqtt_ids")
	var clients []mqtt.Client
	for _, id := range t {
		clients = append(clients, newClient(id))
	}
	return clients
}

// format the iot data
func format(id string, label string, aita string, value float64) string {
	return fmt.Sprintf("%s,%s,%s,%f,%s", id, aita, label, value, time.Now())
}

// publishRaw will publish a message to a mqtt broker
func publishRaw(client mqtt.Client, msg string) {
	topic := viper.GetString("mqtt.topic")
	fmt.Println(topic + " --> " + msg)
	token := client.Publish(topic, byte(viper.GetInt("mqtt.qos")), false, msg)
	token.Wait()
}

// publishRaw will publish a formatted iot message to a mqtt broker
func publish(client mqtt.Client, sensor string, label string, aita string, data float64) {
	opts := client.OptionsReader()
	publishRaw(client, format(opts.ClientID(), label, aita, data))
}
