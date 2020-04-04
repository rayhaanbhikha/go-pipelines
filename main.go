package main

import (
	"bytes"
	"context"
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
	parrallelExec        = 5
	transformDelay       = time.Millisecond * 2e3
	postDelay            = time.Millisecond * 3e3
	maxPipelineDurations = time.Millisecond * 10e3
	jsonServerURL        = "http://localhost:3000/users"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, maxPipelineDurations)
	defer cancel()
	userList := make([]<-chan *user.User, 0)
	errList := make([]<-chan error, 0)

	start := time.Now()

	users, errc, err := read(ctx, "./data-set.csv")
	utils.CheckErr(err)
	errList = append(errList, errc)

	for i := 0; i < parrallelExec; i++ {
		transformedUsers, errc, err := transform(ctx, users)
		utils.CheckErr(err)
		userList = append(userList, transformedUsers)
		errList = append(errList, errc)
	}

	out := utils.Merge(ctx, userList...)

	for i := 0; i < parrallelExec; i++ {
		errc, err = post(ctx, out)
		utils.CheckErr(err)
		errList = append(errList, errc)
	}

	for err := range utils.MergeErr(ctx, errList...) {
		log.Fatal(err)
	}

	fmt.Println("Elapsed time: ", time.Since(start))
}

func read(ctx context.Context, filePath string) (<-chan *user.User, <-chan error, error) {
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
			select {
			case userChan <- user.NewUser(data):
			case <-ctx.Done():
				fmt.Println(ctx.Err().Error())
				return
			}
		}
	}()
	return userChan, errc, nil
}

func transform(ctx context.Context, users <-chan *user.User) (<-chan *user.User, <-chan error, error) {
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
			select {
			case transformedUsers <- user:
			case <-ctx.Done():
				fmt.Println(ctx.Err().Error())
				return
			}
		}
	}()
	return transformedUsers, errc, nil
}

func post(ctx context.Context, users <-chan *user.User) (<-chan error, error) {
	errc := make(chan error)
	go func() {
		defer close(errc)
		for user := range users {
			select {
			case <-ctx.Done():
				fmt.Println(ctx.Err().Error())
				return
			default:
				fmt.Println("posting")
				err := postUser(ctx, user)
				if err != nil {
					errc <- err
					return
				}
			}
		}
	}()
	return errc, nil
}

func postUser(ctx context.Context, user *user.User) error {
	time.Sleep(postDelay)
	buf := bytes.NewReader(user.JSON())
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, "POST", jsonServerURL, buf)
	req.Header.Add("Content-Type", "application/json")
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return nil
}
