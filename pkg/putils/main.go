package putils

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/spf13/viper"
)

// Aita is a list of airport codes
var Aita = []string{"AMS", "ATL", "BKK", "CAN", "CDG", "DEL", "DEN", "DFW", "DXB", "FRA", "HKG", "HND", "ICN", "JFK", "LAX", "LHR", "ORD", "PEK", "PVG", "SIN"}

// Airport is an airport's data
type Airport struct {
	Aita string
	Name string
}

// AitaFull is a list of airport codes with its full name
var AitaFull = []Airport{
	Airport{"AMS", "Amsterdam Airport Schiphol"},
	Airport{"ATL", "Hartsfield–Jackson Atlanta International Airport"},
	Airport{"BKK", "Suvarnabhumi Airport"},
	Airport{"CAN", "Guangzhou Baiyun International Airport"},
	Airport{"CDG", "Paris Charles de Gaulle Airport"},
	Airport{"DEL", "Indira Gandhi International Airport"},
	Airport{"DEN", "Denver International Airport"},
	Airport{"DFW", "Dallas/Fort Worth International Airport"},
	Airport{"DXB", "Dubai International Airport"},
	Airport{"FRA", "Frankfurt am Main Airport"},
	Airport{"HKG", "Hong Kong International Airport"},
	Airport{"HND", "Tokyo International Airport"},
	Airport{"ICN", "Incheon International Airport"},
	Airport{"JFK", "John F. Kennedy International Airport"},
	Airport{"LAX", "Los Angeles International Airport"},
	Airport{"LHR", "Heathrow Airport"},
	Airport{"ORD", "O'Hare International Airport"},
	Airport{"PEK", "Beijing Capital International Airport"},
	Airport{"PVG", "Shanghai Pudong International Airport"},
	Airport{"SIN", "Singapore Changi Airport"},
}

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
	viper.BindEnv("FAKEIOT_POPULATE")
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

// ExtractMsgData incoming from IoT MQTT message
// sample topic: fakeiot/sensors/wind
// sample message: CDG,7.010292,2020-09-30T14:39:51+02:00
func ExtractMsgData(msg string) (string, float64, time.Time) {
	s := strings.Split(msg, ",")
	value, _ := strconv.ParseFloat(s[1], 64)
	t, _ := time.Parse(time.RFC3339, s[len(s)-1])
	return s[0], value, t
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
