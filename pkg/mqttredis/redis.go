package mqttredis

import (
	"fmt"
	"log"

	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/gomodule/redigo/redis"
	"github.com/rigwild/FIL-A2-infrastructures-integration/pkg/putils"
	"github.com/spf13/viper"
)

var redisconn redis.Conn
var mqttclient mqtt.Client

func redisAppend(key string, data string) {
	_, err := redisconn.Do("APPEND", key, data+"\n")
	if err != nil {
		log.Fatal(err)
	}
}

func onMessageReceived(client mqtt.Client, message mqtt.Message) {
	payload := message.Payload()
	spayload := string(payload)
	fmt.Printf("%s <-- %s\n", message.Topic(), payload)

	t := putils.ExtractDateFromMsg(spayload)
	key := viper.GetString("redis.sensor_data_prefix") + putils.TimeToDate(t)
	redisAppend(key, spayload)
}

// RunMqttListenerRedis will run a MQTT client which will save incoming IoT messages to a Redis instance
func RunMqttListenerRedis() {
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)
	putils.LoadConfig()

	// Connect to the Redis instance
	_redisconn, err := redis.Dial("tcp", viper.GetString("redis.endpoint"))
	if err != nil {
		log.Fatal(err)
	}
	redisconn = _redisconn

	// Close the redis connection on exit
	defer redisconn.Close()

	// Connect to the MQTT broker
	_mqttclient := putils.NewMqttClient(viper.GetString("listeners.redis_mqtt_id"))
	mqttclient = _mqttclient

	// MQTT subscribe and react
	putils.SubscribeAndReact(mqttclient, viper.GetString("mqtt.topic"), onMessageReceived)

	// Kill the process on SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
