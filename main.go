package main

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rayhaanbhikha/go-pipelines/user"
	"github.com/rayhaanbhikha/go-pipelines/utils"
)

const (
	parrallelExec  = 5
	transformDelay = time.Millisecond * 2e3
	postDelay      = time.Millisecond * 3e3
)

func main() {
	userList := make([]<-chan *user.User, 0)
	errList := make([]<-chan error, 0)

	start := time.Now()

	users, errc, err := read("./data-set.csv")
	utils.CheckErr(err)
	errList = append(errList, errc)

	for i := 0; i < parrallelExec; i++ {
		transformedUsers, errc, err := transform(users)
		utils.CheckErr(err)
		userList = append(userList, transformedUsers)
		errList = append(errList, errc)
	}

	out := utils.Merge(userList...)

	for i := 0; i < parrallelExec; i++ {
		errc, err = post(out)
		utils.CheckErr(err)
		errList = append(errList, errc)
	}

	for err := range utils.MergeErr(errList...) {
		log.Fatal(err)
	}

	fmt.Println("Elapsed time: ", time.Since(start))
}

func read(filePath string) (<-chan *user.User, <-chan error, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	csvReader := csv.NewReader(file)
	userChan := make(chan *user.User)
	errc := make(chan error)
	go func() {
		defer file.Close()
		defer close(userChan)
		defer close(errc)
		for {
			data, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errc <- err
				return
			}
			userChan <- user.NewUser(data)
		}
	}()
	return userChan, errc, nil
}

func transform(users <-chan *user.User) (<-chan *user.User, <-chan error, error) {
	transformedUsers := make(chan *user.User)
	errc := make(chan error)
	go func() {
		defer close(transformedUsers)
		defer close(errc)
		for user := range users {
			if user.FirstName == "error" {
				errc <- errors.New("Unrecoverable error")
				return
			}
			time.Sleep(transformDelay)
			user.Transform()
			transformedUsers <- user
		}
	}()
	return transformedUsers, errc, nil
}

func post(users <-chan *user.User) (<-chan error, error) {
	errc := make(chan error)
	go func() {
		defer close(errc)
		for user := range users {
			err := postUser(user)
			if err != nil {
				errc <- err
				return
			}
		}
	}()
	return errc, nil
}

func postUser(user *user.User) error {
	time.Sleep(postDelay)
	buf := bytes.NewReader(user.JSON())
	res, err := http.Post("http://localhost:3000/users", "application/json", buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return nil
}
