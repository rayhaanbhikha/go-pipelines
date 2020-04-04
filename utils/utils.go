package utils

import (
	"log"
	"sync"

	"github.com/rayhaanbhikha/go-pipelines/user"
)

// CheckErr ...logs fatal err
func CheckErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Merge ... merge user channels into one output channel.
func Merge(inputs ...<-chan *user.User) <-chan *user.User {
	output := make(chan *user.User)

	var wg sync.WaitGroup
	wg.Add(len(inputs))

	sendToOutput := func(input <-chan *user.User) {
		defer wg.Done()
		for user := range input {
			output <- user
		}
	}

	go func() {
		for _, input := range inputs {
			go sendToOutput(input)
		}
	}()

	go func() {
		defer close(output)
		wg.Wait()
	}()

	return output
}
