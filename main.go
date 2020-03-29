package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rayhaanbhikha/go-pipelines/user"
)

func main() {
	start := time.Now()
	file, err := os.Open("./data-set.csv")
	defer file.Close()
	if err != nil {
		panic(err)
	}
	csvReader := csv.NewReader(file)
	for {
		data, err := csvReader.Read()
		time.Sleep(time.Millisecond * 2e3)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		user := user.NewUser(data)
		transform(user)
		postUser(user)
	}
	fmt.Println("Elapsed time: ", time.Since(start))
}

func transform(user *user.User) {
	time.Sleep(time.Millisecond * 1e3)
	user.FirstName = strings.ToUpper(user.FirstName)
	user.LastName = strings.ToUpper(user.LastName)
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
