package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dgoffredo/go-batch/batch"
)

func main() {
	messageTimeout, batchTimeout, maxBatchSize := parseCommandLine()

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
		MessageTimeout: messageTimeout,
		BatchTimeout:   batchTimeout,
		MaxBatchSize:   maxBatchSize}

	go batch.Consume(context.TODO(), config)

	// Read lines from standard input, sending each line as a message to the
	// batch consumer.
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		messages <- scanner.Text()
	}
	close(messages)
}

func parseCommandLine() (time.Duration, time.Duration, int) {
	var messageTimeout int
	var batchTimeout int
	var maxBatchSize int

	flag.IntVar(&messageTimeout, "messageTimeout", 1000, "message timeout (milliseconds)")
	flag.IntVar(&batchTimeout, "batchTimeout", 5000, "batch timeout (milliseconds)")
	flag.IntVar(&maxBatchSize, "maxBatchSize", 0, "maximum messages per batch")

	flag.Parse()

	ms := func(howMany int) time.Duration {
		return time.Millisecond * time.Duration(howMany)
	}

	return ms(messageTimeout), ms(batchTimeout), maxBatchSize
}
