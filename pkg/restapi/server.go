package restapi

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/mux"
	"github.com/rigwild/FIL-A2-infrastructures-integration/pkg/putils"
	"github.com/spf13/viper"
)

var redisconn redis.Conn

func serverLog(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

func getKey(aita string, date string, sensor string) string {
	return fmt.Sprintf("%s|%s|%s|%s", viper.GetString("redis.sensor_data_prefix"), date, aita, sensor)
}

// AirportHandler will give the list of AITA codes available
func AirportHandler(w http.ResponseWriter, r *http.Request) {
	type APIResponse struct {
		Aitas []string `json:"aitas"`
	}
	json.NewEncoder(w).Encode(APIResponse{putils.Aita})
}

// // AirportAitaHandler will give data TODO
// func AirportAitaHandler(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	fmt.Fprintf(w, "Category: %v\n", vars["aita"])
// }

// AirportAitaDateSensorHandler will give data TODO
// /airports/ATL/date/2020-09-30/sensors/pressure
func AirportAitaDateSensorHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := getKey(vars["aita"], vars["date"], vars["sensor"])

	type SensorData struct {
		D string  `json:"d"`
		V float64 `json:"v"`
	}
	type APIResponse struct {
		Data  []SensorData `json:"data"`
		Min   float64      `json:"min"`
		Max   float64      `json:"max"`
		Avg   float64      `json:"avg"`
		Count float64      `json:"count"`
	}

	_data, _ := redis.String(redisconn.Do("GET", key+"|data"))

	var data []SensorData
	json.Unmarshal([]byte("["+_data[1:]+"]"), &data)
	min, _ := redis.Float64(redisconn.Do("GET", key+"|min"))
	max, _ := redis.Float64(redisconn.Do("GET", key+"|max"))
	avg, _ := redis.Float64(redisconn.Do("GET", key+"|avg"))
	count, _ := redis.Float64(redisconn.Do("GET", key+"|count"))

	json.NewEncoder(w).Encode(APIResponse{data, min, max, avg, count})
}

// AirportAitaDateSensorHandler will give data TODO
// /airports/ATL/year/2020
// /airports/ATL/year/2020/month/09
func AirportAitaYearHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := getKey(vars["aita"], vars["date"], vars["sensor"])

	type SensorData struct {
		D string  `json:"d"`
		V float64 `json:"v"`
	}
	type APIResponse struct {
		Data  []SensorData `json:"data"`
		Min   float64      `json:"min"`
		Max   float64      `json:"max"`
		Avg   float64      `json:"avg"`
		Count float64      `json:"count"`
	}

	_data, _ := redis.String(redisconn.Do("GET", key+"|data"))

	var data []SensorData
	json.Unmarshal([]byte("["+_data[1:]+"]"), &data)
	min, _ := redis.Float64(redisconn.Do("GET", key+"|min"))
	max, _ := redis.Float64(redisconn.Do("GET", key+"|max"))
	avg, _ := redis.Float64(redisconn.Do("GET", key+"|avg"))
	count, _ := redis.Float64(redisconn.Do("GET", key+"|count"))

	json.NewEncoder(w).Encode(APIResponse{data, min, max, avg, count})
}

// StartServer Starts the fakeiot API
func StartServer() {
	putils.LoadConfig()

	// Connect to the Redis instance
	_redisconn, err := redis.Dial("tcp", viper.GetString("redis.endpoint"))
	if err != nil {
		log.Fatal(err)
	}
	redisconn = _redisconn

	// Close the redis connection on exit
	defer redisconn.Close()

	r := mux.NewRouter()
	r.HandleFunc("/airports", AirportHandler).Methods("GET")
	// r.HandleFunc("/airports/{aita}", AirportAitaHandler).Methods("GET")
	// r.HandleFunc("/airports/{aita}/{date}", AirportAitaDateHandler).Methods("GET")
	r.HandleFunc("/airports/{aita}/date/{date}/sensors/{sensor}", AirportAitaDateSensorHandler).Methods("GET")
	r.HandleFunc("/airports/{aita}/year/{year}", AirportAitaYearHandler).Methods("GET")
	r.HandleFunc("/airports/{aita}/year/{year}/month/{month}", AirportAitaYearMonthHandler).Methods("GET")
	http.Handle("/", r)
	err = http.ListenAndServe(":8080", serverLog(http.DefaultServeMux))
	log.Fatal(err)
}
