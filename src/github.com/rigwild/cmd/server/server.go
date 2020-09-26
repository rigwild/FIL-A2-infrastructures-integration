package main

import (
	"fmt"
	"log"
	"net/http"
)

func helloHandler(w http.ResponseWriter, r *http.Request) {
	url := r.
	fmt.Fprintf(w, "hello!")
}

func main() {
	http.HandleFunc("/hello", helloHandler)
	err := http.ListenAndServe(":8080", nil)
	log.Fatal(err)
}
