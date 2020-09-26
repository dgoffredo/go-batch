package batch

import (
	"context"
	"fmt"
	"testing"
	"time"
	// mockable wrapper around time functions
)

// TestDefaultOneBucket verifies that with default limits, Consume flushes each
// message individually in its own batch.
func TestDefaultOneBucket(t *testing.T) {
	// the names "source" and "sink" might seem backwards, but remember that
	// they're from the point of view of Consume.

	// source is where we send individual messages.
	source := make(chan interface{})
	// sink is where we receive flushed batches. It's buffered so that Consume
	// can send multiple values on it without having to wait for this test to
	// receive.
	sink := make(chan []string, 3)
	config := Config{
		Key: func(interface{}) uint64 { return 0 },
		Flush: func(key uint64, messages []interface{}) {
			if key != 0 {
				t.Errorf("unexpected key %v, expected zero", key)
			}
			output := []string{}
			for _, raw := range messages {
				message, ok := raw.(string)
				if !ok {
					t.Errorf("Unexpected message type %T, expected a string", raw)
				}
				output = append(output, message)
			}
			sink <- output
		},
		Source: source} // everything else is defaulted

	go Consume(context.TODO(), config)

	inOrder := func(messages []string) {
		if len(messages) > cap(sink) {
			panic("malformed test: channel capacity too small")
		}

		for _, message := range messages {
			source <- message
		}

		for _, message := range messages {
			batch := <-sink
			if len(batch) != 1 {
				t.Errorf("expected a batch with one message, but got %v", len(batch))
			}
			if batch[0] != message {
				t.Errorf("expected the message %v, but got %v", batch[0], message)
			}
		}
	}

	// send, recv, send, recv, etc.
	inOrder([]string{"hello"})
	inOrder([]string{"world"})
	inOrder([]string{"how"})
	inOrder([]string{"are"})
	inOrder([]string{"you"})

	// send, send, send, ..., recv, recv, recv, etc.
	inOrder([]string{"max", "len", "three"})
	inOrder([]string{"because", "of"})
	inOrder([]string{"channel", "buffer"})
}

// TestBreathingOneBucket verifies that with a short message timeout and a
// longer batch timeout, Consume flushes batches of messages in the expected
// sequence.
func TestBreathingOneBucket(t *testing.T) {
	// the names "source" and "sink" might seem backwards, but remember that
	// they're from the point of view of Consume.

	// source is where we send individual messages.
	source := make(chan interface{})
	// batches is where we accumulate flushed batches.
	batches := [][]string{}
	// mockClock allows us to control the time
	mockClock := newSynchronizedClock()
	config := Config{
		Key: func(interface{}) uint64 { return 0 },
		Flush: func(key uint64, messages []interface{}) {
			if key != 0 {
				t.Errorf("unexpected key %v, expected zero", key)
			}
			output := []string{}
			for _, raw := range messages {
				message, ok := raw.(string)
				if !ok {
					t.Errorf("Unexpected message type %T, expected a string", raw)
				}
				output = append(output, message)
			}
			fmt.Println("flushing ", output)
			batches = append(batches, output)
		},
		Source:         source,
		MessageTimeout: time.Second,
		BatchTimeout:   time.Second * 3,
		// MaxBatchSize is defaulted to zero (no limit).
		Clock: mockClock}

	// done is used by the consuming goroutine to indicate its completion.
	done := make(chan struct{})
	go func() {
		Consume(context.TODO(), config)
		done <- struct{}{}
	}()

	// If messages are far enough apart, then each is flushed individually.
	fmt.Println(1)
	mockClock.Steps <- time.Second + time.Nanosecond
	fmt.Println(2)
	source <- "message timeout puts this in its own batch"
	fmt.Println(3)
	<-mockClock.Notify
	fmt.Println(4)
	mockClock.Steps <- time.Second
	fmt.Println(5)
	source <- "this ends up in its own batch for the same reason"
	fmt.Println(6)
	<-mockClock.Notify
	fmt.Println(7)

	/* TODO
	// If messages are close enough together, then they end up in the same
	// batch. If the burst of messages ends before the batch timeout, then it's
	// the message timeout after the final message that causes the flush.
	source <- "1/5 in a quick burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 200)
	source <- "2/5 in a quick burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 200)
	source <- "3/5 in a quick burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 200)
	source <- "4/5 in a quick burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 200)
	source <- "5/5 in a quick burst"
	mockClock.SyncBeforeAdd(time.Second)

	// If messages are close enough together, then they end up in the same
	// batch. However, if the streak (burst) of messages lasts for longer than
	// the batch timeout, then the batch is flushed and subsequent messages go
	// into the next batch.
	source <- "1/4 in a long burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 750)
	source <- "2/4 in a long burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 750)
	source <- "3/4 in a long burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 750)
	source <- "4/4 in a long burst"
	mockClock.SyncBeforeAdd(time.Millisecond * 750)

	*/
	close(source)
	<-done

	// TODO: assert order
	fmt.Println(batches)
}
