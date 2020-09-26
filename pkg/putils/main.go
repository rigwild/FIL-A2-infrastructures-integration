package putils

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/spf13/viper"
)

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
	viper.SetDefault("mqtt.qos", 1)

	viper.AddConfigPath("configs")
	viper.SetConfigName("config")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n ", err))
	}
}
