package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

func main() {

	fmt.Println("hello world")
	readData("./data-set.csv")
}

type user struct {
	firstName, lastName, email string
}

func NewUser(data []string) *user {
	return &user{firstName: data[0], lastName: data[1], email: data[2]}
}

func readData(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	csvReader := csv.NewReader(file)
	for {
		data, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		fmt.Println(NewUser(data))
	}
}
