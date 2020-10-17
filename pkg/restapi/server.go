package restapi

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/mux"
	"github.com/rigwild/FIL-A2-infrastructures-integration/pkg/putils"
	"github.com/rs/cors"
	"github.com/spf13/viper"
)

func serverLog(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

// AirportHandler will give the list of AITA codes available
func AirportHandler(w http.ResponseWriter, r *http.Request) {
	type APIResponse struct {
		Aitas []putils.Airport `json:"aitas"`
	}
	json.NewEncoder(w).Encode(APIResponse{putils.AitaFull})
}

// AirportAitaDateSensorHandler will give data about sensors in an airport at a precise day (full sensor data history)
// /airports/ATL/date/2020-09-30/sensors/pressure
func AirportAitaDateSensorHandler(w http.ResponseWriter, r *http.Request) {
	redisconn := connectRedis()
	defer redisconn.Close()

	vars := mux.Vars(r)
	key := fmt.Sprintf("%s|%s|%s|%s", viper.GetString("redis.sensor_data_prefix"), vars["date"], vars["aita"], vars["sensor"])

	type SensorData struct {
		D string  `json:"d"`
		V float64 `json:"v"`
	}
	type APIResponse struct {
		Data  []SensorData `json:"data"`
		Min   float64      `json:"min"`
		Max   float64      `json:"max"`
		Avg   float64      `json:"avg"`
		Count int          `json:"count"`
	}

	_data, _ := redis.String(redisconn.Do("GET", key+"|data"))

	var data []SensorData
	json.Unmarshal([]byte("["+_data[1:]+"]"), &data)
	min, _ := redis.Float64(redisconn.Do("GET", key+"|min"))
	max, _ := redis.Float64(redisconn.Do("GET", key+"|max"))
	avg, _ := redis.Float64(redisconn.Do("GET", key+"|avg"))
	count, _ := redis.Int(redisconn.Do("GET", key+"|count"))

	json.NewEncoder(w).Encode(APIResponse{data, min, max, avg, count})
}

// AirportAitaDateStatsHandler will give stats about date at an airport (date can be "total")
// /airports/ATL/dateStats/2020
func AirportAitaDateStatsHandler(w http.ResponseWriter, r *http.Request) {
	redisconn := connectRedis()
	defer redisconn.Close()

	vars := mux.Vars(r)
	key := fmt.Sprintf("%s|%s|%s", viper.GetString("redis.sensor_data_prefix"), vars["dateStats"], vars["aita"])

	type DateData struct {
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
		Avg   float64 `json:"avg"`
		Count int     `json:"count"`
	}

	type APIResponse struct {
		Pressure    DateData `json:"pressure"`
		Temperature DateData `json:"temperature"`
		Wind        DateData `json:"wind"`
		Count       int      `json:"count"`
	}

	_pmin, err := redisconn.Do("GET", key+"|pressure|min")
	if _pmin == nil {
		// No data for this date/aita
		w.WriteHeader(http.StatusNotFound)
		return
	}

	pmin, _ := redis.Float64(_pmin, err)
	pmax, _ := redis.Float64(redisconn.Do("GET", key+"|pressure|max"))
	pavg, _ := redis.Float64(redisconn.Do("GET", key+"|pressure|avg"))
	pcount, _ := redis.Int(redisconn.Do("GET", key+"|pressure|count"))

	wmin, _ := redis.Float64(redisconn.Do("GET", key+"|wind|min"))
	wmax, _ := redis.Float64(redisconn.Do("GET", key+"|wind|max"))
	wavg, _ := redis.Float64(redisconn.Do("GET", key+"|wind|avg"))
	wcount, _ := redis.Int(redisconn.Do("GET", key+"|wind|count"))

	tmin, _ := redis.Float64(redisconn.Do("GET", key+"|temperature|min"))
	tmax, _ := redis.Float64(redisconn.Do("GET", key+"|temperature|max"))
	tavg, _ := redis.Float64(redisconn.Do("GET", key+"|temperature|avg"))
	tcount, _ := redis.Int(redisconn.Do("GET", key+"|temperature|count"))

	count, _ := redis.Int(redisconn.Do("GET", key+"|count"))

	json.NewEncoder(w).Encode(APIResponse{
		DateData{pmin, pmax, pavg, pcount},
		DateData{wmin, wmax, wavg, wcount},
		DateData{tmin, tmax, tavg, tcount},
		count,
	})
}

// DateStatsHandler will give statistics of of a date (date can be "total")
// /dateStats/2020
func DateStatsHandler(w http.ResponseWriter, r *http.Request) {
	redisconn := connectRedis()
	defer redisconn.Close()

	vars := mux.Vars(r)
	key := fmt.Sprintf("%s|%s", viper.GetString("redis.sensor_data_prefix"), vars["dateStats"])

	type DateData struct {
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
		Avg   float64 `json:"avg"`
		Count int     `json:"count"`
	}

	type APIResponse struct {
		Pressure    DateData `json:"pressure"`
		Temperature DateData `json:"temperature"`
		Wind        DateData `json:"wind"`
		Count       int      `json:"count"`
	}

	_pmin, err := redisconn.Do("GET", key+"|pressure|min")
	if _pmin == nil {
		// No data
		w.WriteHeader(http.StatusNotFound)
		return
	}

	pmin, _ := redis.Float64(_pmin, err)
	pmax, _ := redis.Float64(redisconn.Do("GET", key+"|pressure|max"))
	pavg, _ := redis.Float64(redisconn.Do("GET", key+"|pressure|avg"))
	pcount, _ := redis.Int(redisconn.Do("GET", key+"|pressure|count"))

	wmin, _ := redis.Float64(redisconn.Do("GET", key+"|wind|min"))
	wmax, _ := redis.Float64(redisconn.Do("GET", key+"|wind|max"))
	wavg, _ := redis.Float64(redisconn.Do("GET", key+"|wind|avg"))
	wcount, _ := redis.Int(redisconn.Do("GET", key+"|wind|count"))

	tmin, _ := redis.Float64(redisconn.Do("GET", key+"|temperature|min"))
	tmax, _ := redis.Float64(redisconn.Do("GET", key+"|temperature|max"))
	tavg, _ := redis.Float64(redisconn.Do("GET", key+"|temperature|avg"))
	tcount, _ := redis.Int(redisconn.Do("GET", key+"|temperature|count"))

	count, _ := redis.Int(redisconn.Do("GET", viper.GetString("redis.sensor_data_prefix")+"|count"))

	json.NewEncoder(w).Encode(APIResponse{
		DateData{pmin, pmax, pavg, pcount},
		DateData{wmin, wmax, wavg, wcount},
		DateData{tmin, tmax, tavg, tcount},
		count,
	})
}

func connectRedis() redis.Conn {
	redisconn, err := redis.Dial("tcp", viper.GetString("redis.endpoint"))
	if err != nil {
		log.Fatal(err)
	}
	return redisconn
}

// StartServer Starts the fakeiot API
func StartServer() {
	putils.LoadConfig()

	r := mux.NewRouter()
	r.HandleFunc("/airports", AirportHandler).Methods("GET")

	r.HandleFunc("/airports/{aita}/date/{date}/sensors/{sensor}", AirportAitaDateSensorHandler).Methods("GET")
	r.HandleFunc("/airports/{aita}/dateStats/{dateStats}", AirportAitaDateStatsHandler).Methods("GET")
	r.HandleFunc("/dateStats/{dateStats}", DateStatsHandler).Methods("GET")

	http.Handle("/", r)

	fmt.Println("The server is listening on http://localhost:8080")

	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
	})

	err := http.ListenAndServe(":8080", c.Handler(serverLog(http.DefaultServeMux)))
	log.Fatal(err)
}
