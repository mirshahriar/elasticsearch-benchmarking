package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/golang-collections/go-datastructures/queue"
	elastic "gopkg.in/olivere/elastic.v5"
)

const (
	index   = "student"
	docType = "score"
)

// Data ...
type Data struct {
	VarA   float64    `json:"var_a"`
	VarB   int        `json:"var_b"`
	VarC   int        `json:"var_c"`
	VarD   bool       `json:"var_d"`
	Nested NestedData `json:"nested_data"`
}

// NestedData ...
type NestedData struct {
	VarA string `json:"var_a"`
	VarB string `json:"var_b"`
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandStringRunes ...
func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// GenerateRandomData ...
func GenerateRandomData() *Data {
	return &Data{
		VarA: rand.Float64(),
		VarB: rand.Intn(100),
		VarC: rand.Intn(1000),
		VarD: rand.Intn(2) == 0,
		Nested: NestedData{
			VarA: RandStringRunes(10),
			VarB: RandStringRunes(10),
		},
	}
}

const size = 50000

// Populate ...
func Populate() {
	exists, err := elasticClient.IndexExists(index).Do(context.Background())
	if err != nil {
		panic(err)
	}
	if !exists {
		_, err := elasticClient.CreateIndex(index).Do(ctx)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < size; i++ {
		_, err := elasticClient.Get().
			Index(index).
			Type(docType).
			Id(fmt.Sprintf("%v", i)).
			Do(ctx)

		if err != nil {
			_, err := elasticClient.Index().
				Index(index).
				Type(docType).
				Id(fmt.Sprintf("%v", i)).
				BodyJson(GenerateRandomData()).
				Do(ctx)
			if err != nil {
				panic(err)
			}
		}
	}
}

type lazyUpdateTuple struct {
	id  string
	doc *Data
}

var lazyDriverPatchQueue = new(queue.Queue)

// BulkUpdate ...
func BulkUpdate(id string, payload *Data) error {
	lazyDriverPatchQueue.Put(
		lazyUpdateTuple{id, payload},
	)
	return nil
}

// LazyPatchDrivers ...
func LazyPatchDrivers() {
	tick := time.Tick(time.Second * 1)

	for {
		select {
		case <-tick:
			queueLen := lazyDriverPatchQueue.Len()
			if queueLen == 0 {
				continue
			}
			driversToWrite, fetchError := lazyDriverPatchQueue.Get(queueLen)
			if fetchError != nil {
				fmt.Errorf("Bulk Writing Failed %v", fetchError)
				continue
			}

			driverMap := make(map[string]interface{})
			for _, singleDriverPatch := range driversToWrite {
				driverMap[singleDriverPatch.(lazyUpdateTuple).id] = singleDriverPatch.(lazyUpdateTuple).doc
			}
			fmt.Println("Writing in bulk entries: driverMapLen=", len(driverMap), ", queueLen=", queueLen)

			bulkRequest := elasticClient.Bulk()
			for id, doc := range driverMap {
				bulkRequest.Add(
					elastic.NewBulkUpdateRequest().
						Index(index).
						Type(docType).
						Id(id).
						RetryOnConflict(2).
						Doc(doc),
				)
			}
			_, insertErr := bulkRequest.Do(ctx)
			if insertErr != nil {
				fmt.Errorf("bulk error: %v", insertErr)
			}
		}
	}
}
