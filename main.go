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
	"github.com/rayhaanbhikha/go-pipelines/utils"
)

const parrallelExec = 5

func main() {
	start := time.Now()

	users := read("./data-set.csv")

	userList := make([]<-chan *user.User, 0)

	for i := 0; i < parrallelExec; i++ {
		transformedUsers := transform(users)
		userList = append(userList, transformedUsers)
	}

	out := utils.Merge(userList...)

	post(out)

	fmt.Println("Elapsed time: ", time.Since(start))
}

func read(filePath string) <-chan *user.User {
	file, err := os.Open(filePath)
	utils.CheckErr(err)
	csvReader := csv.NewReader(file)
	userChan := make(chan *user.User)
	go func() {
		defer file.Close()
		defer close(userChan)
		for {
			data, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			utils.CheckErr(err)
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
			time.Sleep(time.Millisecond * 3e3)
			user.Transform()
			transformedUsers <- user
		}
	}()
	return transformedUsers
}

func post(users <-chan *user.User) {
	var wg sync.WaitGroup
	for currentUser := range users {
		wg.Add(1)
		go func(user *user.User) {
			defer wg.Done()
			postUser(user)
		}(currentUser)
	}
	wg.Wait()
}

func postUser(user *user.User) {
	time.Sleep(3e3 * time.Millisecond)
	buf := bytes.NewReader(user.JSON())
	res, err := http.Post("http://localhost:3000/users", "application/json", buf)
	utils.CheckErr(err)
	defer res.Body.Close()
}
