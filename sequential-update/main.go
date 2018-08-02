package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"

	"gopkg.in/olivere/elastic.v5"
)

var (
	elasticClient *elastic.Client
	ctx           = context.Background()
)

func init() {
	os.Setenv("ELASTIC_URL", "http://0.0.0.0:32769")
	var err error
	elasticClient, err = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(os.Getenv("ELASTIC_URL")))
	if err != nil {
		panic(err)
	}

	Populate()
}

func main() {
	http.HandleFunc("/update", func(w http.ResponseWriter, r *http.Request) {
		id := rand.Intn(size)
		if err := SequentialUpdate(fmt.Sprintf("%v", id), GenerateRandomData()); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})

	fmt.Println("Running...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
