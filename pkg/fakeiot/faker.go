package fakeiot

import (
	"fmt"

	"github.com/rigwild/FIL-A2-infrastructures-integration/pkg/putils"
	"github.com/spf13/viper"
)

// RunFakeiot will send fake IoT data to the MQTT broker
func RunFakeiot() {
	// Debug logs
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)

	putils.LoadConfig()
	sleep := viper.GetInt("fakeiot.broadcast_interval")

	aita := []string{"ATL", "PEK", "LAX", "DXB", "HND", "ORD", "LHR", "PVG", "CDG", "DFW", "CAN", "AMS", "HKG", "ICN", "FRA", "DEN", "DEL", "SIN", "BKK", "JFK"}
	clients := mqttConnect()

	for {
		fmt.Println("Sending fake IoT data to the MQTT broker")
		for i, client := range clients {
			publish(client, "pressure", "Atmospheric pressure", aita[i], putils.RandFloat(1048, 1053))
			publish(client, "temp", "Temperature", aita[i], putils.RandFloat(19, 28))
			publish(client, "wind", "Wind speed", aita[i], putils.RandFloat(4, 8))
		}
		putils.Sleep(sleep)
	}
}
