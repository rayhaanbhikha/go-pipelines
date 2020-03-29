package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/rayhaanbhikha/go-pipelines/user"
)

func main() {
	start := time.Now()
	users := genUserChannel("./data-set.csv")

	transformedUsers := transform(users)

	post(transformedUsers)

	fmt.Println("Elapsed time: ", time.Since(start))
}

func genUserChannel(filePath string) <-chan *user.User {
	file, err := os.Open("./data-set.csv")
	if err != nil {
		panic(err)
	}
	csvReader := csv.NewReader(file)
	userChan := make(chan *user.User)
	go func() {
		defer file.Close()
		defer close(userChan)
		for {
			data, err := csvReader.Read()
			time.Sleep(time.Millisecond * 2e3)
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			userChan <- user.NewUser(data)
		}
	}()
	return userChan
}

func transform(users <-chan *user.User) <-chan *user.User {
	transformedUsers := make(chan *user.User)
	go func() {
		defer close(transformedUsers)
		for user := range users {
			time.Sleep(time.Millisecond * 1e3)
			user.Transform()
			transformedUsers <- user
		}
	}()
	return transformedUsers
}

func post(users <-chan *user.User) {
	var wg sync.WaitGroup
	for user := range users {
		wg.Add(1)
		user := user
		go func() {
			defer wg.Done()
			postUser(user)
		}()
	}
	wg.Wait()
}

func postUser(user *user.User) {
	time.Sleep(2e3 * time.Millisecond)
	buf := bytes.NewReader(user.JSON())
	res, err := http.Post("http://localhost:3000/users", "application/json", buf)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
}
