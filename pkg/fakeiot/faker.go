package fakeiot

import (
	"fmt"
	"strings"
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
func publishRaw(client mqtt.Client, topic string, msg string) {
	opts := client.OptionsReader()
	fmt.Println("[" + opts.ClientID() + "] " + topic + " --> " + msg)

	token := client.Publish(topic, byte(viper.GetInt("mqtt.qos")), false, msg)
	token.Wait()
}

// publishRaw will publish a formatted iot message to a mqtt broker
func publish(client mqtt.Client, sensor string, aita string, data float64) {
	s := fmt.Sprintf("%s,%f,%s", aita, data, time.Now().Format(time.RFC3339))
	publishRaw(client, viper.GetString("mqtt.topic")+"/"+sensor, s)
}

func fakeData() {
	putils.LoadConfig()
	opts := mqtt.NewClientOptions()
	opts.AddBroker(viper.GetString("mqtt.endpoint"))
	opts.SetClientID("fake-data-1")
	opts.SetCleanSession(true)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	c := 0
	for i := 10; i <= 30; i++ { // Day
		for k := 6; k < 9; k++ { // Month
			for l := 2017; l < 2020; l++ { // Year
				for j := 0; j < 5; j++ {
					t := fmt.Sprint(time.Now().Format(time.RFC3339))
					t = strings.Split(t, "T")[1]
					t = fmt.Sprint(l) + "-0" + fmt.Sprint(k) + "-" + fmt.Sprint(i) + "T" + t
					publishRaw(client, viper.GetString("mqtt.topic")+"/pressure", fmt.Sprintf("%s,%f,%s", putils.Aita[j], putils.RandFloat(1048, 1053), t))
					publishRaw(client, viper.GetString("mqtt.topic")+"/temperature", fmt.Sprintf("%s,%f,%s", putils.Aita[j], putils.RandFloat(19, 28), t))
					publishRaw(client, viper.GetString("mqtt.topic")+"/wind", fmt.Sprintf("%s,%f,%s", putils.Aita[j], putils.RandFloat(4, 8), t))
					c++
				}
			}
		}
	}
	fmt.Println("----")
	fmt.Println("Sent " + fmt.Sprint(c) + " messages!")
}

// RunFakeiot will send fake IoT data to the MQTT broker
func RunFakeiot() {
	// Debug logs
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)

	putils.LoadConfig()

	// Environment variable to send fake data accross the month
	if viper.Get("FAKEIOT_POPULATE") == "1" {
		fakeData()
		return
	}

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
