package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/rayhaanbhikha/go-pipelines/user"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*30e3)
	defer cancel()

	var usercList []<-chan *user.User
	var errcList []<-chan error

	users, errc, err := genUserChannel(ctx, "./data-set.csv")
	checkErr(err)
	errcList = append(errcList, errc)

	users, errc, err = transform(ctx, users)
	checkErr(err)
	usercList = append(usercList, users)
	errcList = append(errcList, errc)

	users, errc, err = transform(ctx, users)
	checkErr(err)
	usercList = append(usercList, users)
	errcList = append(errcList, errc)

	out := mergeUserChans(ctx, usercList...)

	errc, err = post(ctx, out)
	checkErr(err)
	errcList = append(errcList, errc)

	errc, err = post(ctx, out)
	checkErr(err)
	errcList = append(errcList, errc)

	for e := range mergeErrChans(ctx, errcList...) {
		fmt.Println("hello: ", e)
		cancel()
	}

	fmt.Println("Elapsed time: ", time.Since(start))
}

// source stage
func genUserChannel(ctx context.Context, filePath string) (<-chan *user.User, <-chan error, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	csvReader := csv.NewReader(file)
	userChan := make(chan *user.User)
	errC := make(chan error, 1)
	go func() {
		defer file.Close()
		defer close(userChan)
		defer close(errC)
		for {
			data, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errC <- err
				return
			}
			select {
			case userChan <- user.NewUser(data):
			case <-ctx.Done():
				fmt.Println(ctx.Err().Error())
				break
			}
		}
	}()
	return userChan, errC, nil
}

// transformer stage
func transform(ctx context.Context, users <-chan *user.User) (<-chan *user.User, <-chan error, error) {
	transformedUsers := make(chan *user.User)
	errC := make(chan error, 1)
	go func() {
		defer close(transformedUsers)
		defer close(errC)
		for user := range users {
			time.Sleep(time.Millisecond * 2e3)
			fmt.Println("transform")
			user.Transform()
			select {
			case transformedUsers <- user:
			case <-ctx.Done():
				return
			}
		}
	}()
	return transformedUsers, errC, nil
}

// sink stage
func post(ctx context.Context, users <-chan *user.User) (<-chan error, error) {
	errC := make(chan error, 1)
	go func() {
		defer close(errC)
		for user := range users {
			fmt.Println("post user", len(users))
			err := postUser(user)
			if err != nil {
				errC <- err
				return
			}
		}
	}()
	return errC, nil
}

func postUser(user *user.User) error {
	time.Sleep(3e3 * time.Millisecond)
	buf := bytes.NewReader(user.JSON())
	res, err := http.Post("http://localhost:3000/users", "application/json", buf)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return nil
}
