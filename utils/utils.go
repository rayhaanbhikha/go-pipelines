package utils

import (
	"context"
	"fmt"
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
func Merge(ctx context.Context, inputs ...<-chan *user.User) <-chan *user.User {
	output := make(chan *user.User)

	var wg sync.WaitGroup
	wg.Add(len(inputs))

	sendToOutput := func(input <-chan *user.User) {
		defer wg.Done()
		for user := range input {
			select {
			case output <- user:
			case <-ctx.Done():
				fmt.Println(ctx.Err().Error())
				return
			}
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

// MergeErr ... merge error channels into one output channel.
func MergeErr(ctx context.Context, inputs ...<-chan error) <-chan error {
	output := make(chan error)

	var wg sync.WaitGroup
	wg.Add(len(inputs))

	sendToOutput := func(input <-chan error) {
		defer wg.Done()
		for err := range input {
			select {
			case output <- err:
			case <-ctx.Done():
				fmt.Println(ctx.Err().Error())
				return
			}
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
