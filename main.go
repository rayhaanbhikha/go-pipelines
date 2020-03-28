package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strings"
)

func main() {
	users := readData("./data-set.csv")

	transform(users)

	writeToFile("users.json", users)
}

type user struct {
	FirstName string
	LastName  string
	Email     string
}

func NewUser(data []string) *user {
	return &user{FirstName: data[0], LastName: data[1], Email: data[2]}
}

func readData(filePath string) []*user {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	csvReader := csv.NewReader(file)
	users := make([]*user, 0)
	for {
		data, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		users = append(users, NewUser(data))
	}
	return users
}

func transform(users []*user) {
	for _, user := range users {
		user.FirstName = strings.ToUpper(user.FirstName)
		user.LastName = strings.ToUpper(user.LastName)
	}
}

func writeToFile(fileName string, users []*user) {
	file, err := os.Create(fileName)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	file.Write([]byte{'['})
	encoder := json.NewEncoder(file)
	for _, user := range users {
		err := encoder.Encode(user)
		file.Write([]byte{','})
		if err != nil {
			panic(err)
		}
	}
	file.Write([]byte{']'})
}
