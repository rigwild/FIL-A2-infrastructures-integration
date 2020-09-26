package restapi

import (
	"fmt"
	"log"
	"net/http"
)

func serverLog(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
		handler.ServeHTTP(w, r)
	})
}

func helloHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello %s!", r.URL.Path)
}

// StartServer Starts the fakeiot API
func StartServer() {
	http.HandleFunc("/hello", helloHandler)
	err := http.ListenAndServe(":8080", serverLog(http.DefaultServeMux))
	log.Fatal(err)
}
