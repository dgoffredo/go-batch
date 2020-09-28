package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dgoffredo/go-batch/batch"
)

func main() {
	messages := make(chan interface{})

	config := batch.Config{
		Key: func(message interface{}) uint64 {
			// There's a separate bucket for each string length.
			return uint64(len(message.(string)))
		},
		Flush: func(key uint64, messages []interface{}) {
			fmt.Println("flushed bucket for strings of length ", key, ": ", messages)
		},
		Source:         messages,
		MessageTimeout: time.Second,
		BatchTimeout:   time.Second * 6}

	go batch.Consume(context.TODO(), config)

	// Read lines from standard input, sending each line as a message to the
	// batch consumer.
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		messages <- scanner.Text()
	}
	close(messages)
}
