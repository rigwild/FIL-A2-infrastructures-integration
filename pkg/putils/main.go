package putils

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/viper"
)

// Aita is a list of airport codes
var Aita = []string{"ATL", "PEK", "LAX", "DXB", "HND", "ORD", "LHR", "PVG", "CDG", "DFW", "CAN", "AMS", "HKG", "ICN", "FRA", "DEN", "DEL", "SIN", "BKK", "JFK"}

// RandFloat between min and max
func RandFloat(min float64, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

// Sleep <ms> milliseconds
func Sleep(ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// LoadConfig from the TOML config file
func LoadConfig() {
	viper.SetDefault("fakeiot.broadcast_interval", 10000)

	viper.SetDefault("mqtt.endpoint", "tcp://localhost:18830")
	viper.SetDefault("mqtt.qos", 1)
	viper.SetDefault("mqtt.topic", "fakeiot/sensors")

	viper.SetDefault("redis.endpoint", "localhost:6379")
	viper.SetDefault("redis.sensor_data_prefix", "sensor:")

	viper.SetDefault("listeners.file_logs_dir", "./logs")
	viper.SetDefault("listeners.file_mqtt_id", "listener-file-1")
	viper.SetDefault("listeners.redis_mqtt_id", "listener-redis-1")

	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n ", err))
	}
}

// ExtractAitaSensorFromMsg incoming from MQTT
// sample: `fakeiot-10,CDG,wind,7.010292,2020-09-28T23:36:35+02:00`
func ExtractAitaSensorFromMsg(msg string) (string, string) {
	s := strings.Split(msg, ",")
	return s[1], s[2]
}

// ExtractDateFromMsg incoming from MQTT
// sample: `fakeiot-10,CDG,wind,7.010292,2020-09-28T23:36:35+02:00`
func ExtractDateFromMsg(msg string) time.Time {
	s := strings.Split(msg, ",")
	t, _ := time.Parse(time.RFC3339, s[len(s)-1])
	return t
}

// TimeToDate will convert a golang `time.Now()` to a YYYY-MM-DD date
func TimeToDate(t time.Time) string {
	return fmt.Sprintf("%d-%02d-%02d", t.Year(), t.Month(), t.Day())
}

// NewMqttClient will start a new mqtt client
func NewMqttClient(clientID string) mqtt.Client {
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

// SubscribeAndReact will subscribe an MQTT client to a topic and execute a function on incoming message
func SubscribeAndReact(mqttclient mqtt.Client, topic string, onMessageReceived func(client mqtt.Client, message mqtt.Message)) {
	if token := mqttclient.Subscribe(topic, 0, onMessageReceived); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Println("Connected to MQTT broker.")
		fmt.Println("Subscribed to the topic \"" + topic + "\". Reacting to incoming messages.")
	}
}
