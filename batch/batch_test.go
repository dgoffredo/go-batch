package batch

import (
	"context"
	"testing"
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
