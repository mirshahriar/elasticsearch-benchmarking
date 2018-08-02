package main

import (
	"context"
	"fmt"
	"math/rand"
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

const size = 200

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
				Routing(fmt.Sprintf("%v", i%5)).
				Id(fmt.Sprintf("%v", i)).
				BodyJson(GenerateRandomData()).
				Do(ctx)
			if err != nil {
				panic(err)
			}
		}
	}
}

// SequentialUpdate ...
func SequentialUpdate(id int, payload interface{}) error {
	_, err := elasticClient.Update().
		Index(index).
		Type(docType).
		Routing(fmt.Sprintf("%v", (id % 5))).
		Id(fmt.Sprintf("%v", id)).
		RetryOnConflict(2).
		Doc(payload).
		Do(ctx)
	return err
}
