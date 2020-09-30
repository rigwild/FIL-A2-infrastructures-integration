package mqttredis

import (
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/gomodule/redigo/redis"
	"github.com/rigwild/FIL-A2-infrastructures-integration/pkg/putils"
	"github.com/spf13/viper"
)

var redisconn redis.Conn
var mqttclient mqtt.Client

func redisAppend(key string, time time.Time, value float64) {
	_, err := redisconn.Do("APPEND", key, fmt.Sprintf(",{\"v\":%f,\"d\":\"%s\"}", value, time))
	if err != nil {
		log.Fatal(err)
	}
}

func redisAvg(key string, data float64) {
	temp, err := redis.Float64(redisconn.Do("GET", key))
	if err == nil {
		data = (data + temp) / 2
	}
	_, err = redisconn.Do("SET", key, data)
	if err != nil {
		log.Fatal(err)
	}
}

func redisMin(key string, data float64) {
	temp, err := redis.Float64(redisconn.Do("GET", key))
	if err == nil {
		data = math.Min(temp, data)
	}
	_, err = redisconn.Do("SET", key, data)
	if err != nil {
		log.Fatal(err)
	}
}

func redisMax(key string, data float64) {
	temp, err := redis.Float64(redisconn.Do("GET", key))
	if err == nil {
		data = math.Max(temp, data)
	}
	_, err = redisconn.Do("SET", key, data)
	if err != nil {
		log.Fatal(err)
	}
}

func redisIncr(key string) {
	_, err := redisconn.Do("INCR", key)
	if err != nil {
		log.Fatal(err)
	}
}

func onMessageReceived(client mqtt.Client, message mqtt.Message) {
	payload := message.Payload()
	topic := message.Topic()
	fmt.Printf("%s <-- %s\n", topic, payload)

	sensor := strings.Split(topic, viper.GetString("mqtt.topic")+"/")[1]
	aita, value, _t := putils.ExtractMsgData(string(payload))
	date := putils.TimeToDate(_t)

	sensorDataPrefix := viper.GetString("redis.sensor_data_prefix")

	// --- Airport sensor stats
	fullkeyprefix := []string{
		// sensor|2020-09-30|CDG|wind -> sensor|2020-09-30|CDG|wind|avg <-- IoT data goes here (sensor|2020-09-30|CDG|wind|data)
		fmt.Sprintf("%s|%s|%s|%s", sensorDataPrefix, date, aita, sensor),
		// sensor|2020-09|CDG|wind -> sensor|2020-09|CDG|wind|avg
		fmt.Sprintf("%s|%s|%s|%s", sensorDataPrefix, date[:7], aita, sensor),
		// sensor|2020|CDG|wind -> sensor|2020|CDG|wind|avg
		fmt.Sprintf("%s|%s|%s|%s", sensorDataPrefix, date[:4], aita, sensor),
		// sensor|total|CDG|wind -> sensor|total|CDG|wind|avg
		fmt.Sprintf("%s|total|%s|%s", sensorDataPrefix, aita, sensor),
	}

	// --- Airport common stats (counts)
	airportkeyprefix := []string{
		// sensor|2020-09-30|CDG -> sensor|2020-09-30|CDG|count (only)
		fmt.Sprintf("%s|%s|%s", sensorDataPrefix, date, aita),
		// sensor|2020-09|CDG -> sensor|2020-09|CDG|count (only)
		fmt.Sprintf("%s|%s|%s", sensorDataPrefix, date[:7], aita),
		// sensor|2020|CDG -> sensor|2020|CDG|count (only)
		fmt.Sprintf("%s|%s|%s", sensorDataPrefix, date[:4], aita),
		// sensor|total|CDG -> sensor|total|CDG|count (only)
		fmt.Sprintf("%s|total|%s", sensorDataPrefix, aita),
	}

	// --- Global sensors stats
	globalkeysensorprefix := []string{
		// sensor|2020-09-30|wind -> sensor|2020-09-30|wind|avg
		fmt.Sprintf("%s|%s|%s", sensorDataPrefix, date, sensor),
		// sensor|2020-09|wind -> sensor|2020-09|wind|avg
		fmt.Sprintf("%s|%s|%s", sensorDataPrefix, date[:7], sensor),
		// sensor|2020|wind -> sensor|2020|wind|avg
		fmt.Sprintf("%s|%s|%s", sensorDataPrefix, date[:4], sensor),
		// sensor|total|wind -> sensor|total|wind|avg
		fmt.Sprintf("%s|total|%s", sensorDataPrefix, sensor),
	}

	// --- Global common stats (counts)
	globalkeyprefix := []string{
		// sensor|2020-09-30 -> sensor|2020-09-30|count (only)
		fmt.Sprintf("%s|%s", sensorDataPrefix, date),
		// sensor|2020-09 -> sensor|2020-09|count (only)
		fmt.Sprintf("%s|%s", sensorDataPrefix, date[:7]),
		// sensor|2020 -> sensor|2020|count (only)
		fmt.Sprintf("%s|%s", sensorDataPrefix, date[:4]),
		// sensor|total -> sensor|total|count (only)
		fmt.Sprintf("%s", sensorDataPrefix),
	}

	// Append IoT data
	redisAppend(fullkeyprefix[0]+"|data", _t, value)

	// Refresh average, minimum and maximum data
	for _, v := range fullkeyprefix {
		redisAvg(v+"|avg", value)
		redisMin(v+"|min", value)
		redisMax(v+"|max", value)
		redisIncr(v + "|count")
	}
	for _, v := range globalkeysensorprefix {
		redisAvg(v+"|avg", value)
		redisMin(v+"|min", value)
		redisMax(v+"|max", value)
		redisIncr(v + "|count")
	}

	for _, v := range airportkeyprefix {
		redisIncr(v + "|count")
	}
	for _, v := range globalkeyprefix {
		redisIncr(v + "|count")
	}
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
	putils.SubscribeAndReact(mqttclient, viper.GetString("mqtt.topic")+"/#", onMessageReceived)

	// Kill the process on SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
