package main

import (
	"bytes"
	"context"
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10e3)
	defer cancel()
	users := genUserChannel(ctx, "./data-set.csv")

	tU1 := transform(ctx, users)
	tU2 := transform(ctx, users)
	tU3 := transform(ctx, users)
	tU5 := transform(ctx, users)
	tU4 := transform(ctx, users)

	out := merge(ctx, tU1, tU2, tU3, tU4, tU5)
	post(ctx, out)

	fmt.Println("Elapsed time: ", time.Since(start))
}

func merge(ctx context.Context, input ...<-chan *user.User) <-chan *user.User {

	output := make(chan *user.User)

	var wg sync.WaitGroup
	wg.Add(len(input))

	join := func(input <-chan *user.User) {
		defer wg.Done()
		fmt.Println("waiting")
		for user := range input {
			select {
			case output <- user:
			case <-ctx.Done():
				return
			}
		}
	}

	for _, inputChan := range input {
		go join(inputChan)
	}

	go func() {
		defer close(output)
		wg.Wait()
	}()

	return output
}

func genUserChannel(ctx context.Context, filePath string) <-chan *user.User {
	file, err := os.Open(filePath)
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
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}
			select {
			case userChan <- user.NewUser(data):
			case <-ctx.Done():
				fmt.Println(ctx.Err().Error())
				break
			}
		}
	}()
	return userChan
}

func transform(ctx context.Context, users <-chan *user.User) <-chan *user.User {
	transformedUsers := make(chan *user.User)
	go func() {
		defer close(transformedUsers)
		for user := range users {
			fmt.Println("transforming")
			time.Sleep(time.Millisecond * 2e3)
			user.Transform()
			select {
			case transformedUsers <- user:
			case <-ctx.Done():
				return
			}
		}
	}()
	return transformedUsers
}

func post(ctx context.Context, users <-chan *user.User) {
	var wg sync.WaitGroup
	for user := range users {
		select {
		case <-ctx.Done():
			return
		default:
			wg.Add(1)
			user := user
			go func() {
				defer wg.Done()
				fmt.Println("post user")
				postUser(user)
			}()
		}
	}
	wg.Wait()
}

func postUser(user *user.User) {
	time.Sleep(3e3 * time.Millisecond)
	buf := bytes.NewReader(user.JSON())
	res, err := http.Post("http://localhost:3000/users", "application/json", buf)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
}
