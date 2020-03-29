package main

import (
	"context"
	"sync"

	"github.com/rayhaanbhikha/go-pipelines/user"
)

func mergeUserChans(ctx context.Context, input ...<-chan *user.User) <-chan *user.User {

	output := make(chan *user.User, 5)

	var wg sync.WaitGroup
	wg.Add(len(input))

	join := func(input <-chan *user.User) {
		defer wg.Done()
		// fmt.Println("merging user chans")
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

func mergeErrChans(ctx context.Context, input ...<-chan error) <-chan error {

	output := make(chan error)

	var wg sync.WaitGroup
	wg.Add(len(input))

	join := func(input <-chan error) {
		defer wg.Done()
		// fmt.Println("merging error chans")
		for i := range input {
			select {
			case output <- i:
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
