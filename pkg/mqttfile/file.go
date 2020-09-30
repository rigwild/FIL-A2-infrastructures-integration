package mqttfile

import (
	"fmt"
	"log"
	"strings"

	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/rigwild/FIL-A2-infrastructures-integration/pkg/putils"
	"github.com/spf13/viper"
)

var mqttclient mqtt.Client

func writeFile(filePath string, txt string) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(txt + "\n"); err != nil {
		log.Println(err)
	}
}

func onMessageReceived(client mqtt.Client, message mqtt.Message) {
	payload := message.Payload()
	topic := message.Topic()
	fmt.Printf("%s <-- %s\n", topic, payload)

	sensor := strings.Split(topic, viper.GetString("mqtt.topic")+"/")[1]
	aita, value, _t := putils.ExtractMsgData(string(payload))
	t := putils.TimeToDate(_t)

	filePath := filepath.Join(viper.GetString("listeners.file_logs_dir"), aita+"-"+t+"-"+sensor+".csv")
	writeFile(filePath, fmt.Sprintf("%s,%f,%s", aita, value, _t))
}

// RunMqttListenerFile will run a MQTT client which will save incoming IoT messages to CSV files
func RunMqttListenerFile() {
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)
	putils.LoadConfig()

	// mkdir -p logs_dir
	os.MkdirAll(viper.GetString("listeners.file_logs_dir"), os.ModePerm)

	// Connect to the MQTT broker
	_mqttclient := putils.NewMqttClient(viper.GetString("listeners.redis_mqtt_id"))
	mqttclient = _mqttclient

	// MQTT subscribe to all sensors and react
	topic := viper.GetString("mqtt.topic") + "/#"
	putils.SubscribeAndReact(mqttclient, topic, onMessageReceived)

	// Kill the process on SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}
