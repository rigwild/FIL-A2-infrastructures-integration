package fakeiot

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rigwild/FIL-A2-infrastructures-integration/pkg/putils"
	"github.com/spf13/viper"
)

// mqttConnect will start all mqtt clients
func mqttConnect() []mqtt.Client {
	t := viper.GetStringSlice("fakeiot.mqtt_ids")
	var clients []mqtt.Client
	for _, id := range t {
		clients = append(clients, putils.NewMqttClient(id))
	}
	return clients
}

// publishRaw will publish a message to a mqtt broker
func publishRaw(client mqtt.Client, msg string) {
	topic := viper.GetString("mqtt.topic")
	fmt.Println(topic + " --> " + msg)
	token := client.Publish(topic, byte(viper.GetInt("mqtt.qos")), false, msg)
	token.Wait()
}

// publishRaw will publish a formatted iot message to a mqtt broker
func publish(client mqtt.Client, sensor string, aita string, data float64) {
	opts := client.OptionsReader()
	s := fmt.Sprintf("%s,%s,%s,%f,%s", opts.ClientID(), aita, sensor, data, time.Now().Format(time.RFC3339))
	publishRaw(client, s)
}

// RunFakeiot will send fake IoT data to the MQTT broker
func RunFakeiot() {
	// Debug logs
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)

	putils.LoadConfig()
	sleep := viper.GetInt("fakeiot.broadcast_interval")

	// Connect all fake IoT MQTT clients
	clients := mqttConnect()

	for {
		fmt.Println("--- Sending fake IoT data to the MQTT broker")
		for i, client := range clients {
			publish(client, "pressure", putils.Aita[i], putils.RandFloat(1048, 1053))
			publish(client, "temperature", putils.Aita[i], putils.RandFloat(19, 28))
			publish(client, "wind", putils.Aita[i], putils.RandFloat(4, 8))
		}
		putils.Sleep(sleep)
	}
}
