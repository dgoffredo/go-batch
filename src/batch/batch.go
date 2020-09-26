// Package batch provides facilities for processing messages in batches, where
// a batch is "flushed" when some condition is met, such as a timeout.
//
// Suppose that you are designing an "update translator" for modifications to
// some data, such as an online dating profile.
//
// "Update events" are produced by one system, and your "update translator"
// translates the events into updates suitable for another (perhaps newer)
// system, and then sends those updates to the other system.
//
//     +-----------+      +-----+      +------------+
//     | upstream  | ---> | you | ---> | downstream |
//     | producer  |      +-----+      |  service   |
//     +-----------+                   +------------+
//
// The events produced by the upstream system can often be combined by your
// translator before being sent downstream. For example, if a user adds a
// religion to his profile, changes his age preference, and updates his
// birthday, then the upstream producer might emit several events, possibly in
// bursts. All of those events, or any contiguous subsequence of them, could be
// combined by the translator before sending the combined message downstream.
//
// In order to strike a balance between volume reduction (large batches) and
// latency reduction (small batches), this package uses a batching scheme
// involving two timeouts and a size limit:
//
// - BatchTimeout: If a batch is non-empty for this amount of time, then it is
//   flushed.
// - MessageTimeout: If this amount of time elapses after receiving a message
//   and before receiving the next message, then the batch is flushed.
// - MaxBatchSize: If a batch accumulates more than this many messages, then it
//   is flushed.
//
// The advantage of having two timeouts is that latency can be reduced on
// average while still allowing for "long runs" of closely-spaced messages to
// be combined. Typically MessageTimeout is short, while BatchTimeout is
// longer.
//
// See main.go for a usage example.
package batch

import (
	"container/heap"
	"context"
	"time"
)

// Config specifies the behavior of Consume. Consume receives messages from a
// channel, deals the messages into buckets, and periodically flushes the buckets.
type Config struct {
	// Key calculates the bucket key for the specified message.
	Key func(message interface{}) (key uint64)
	// Flush handles the specified messages, all of which are associated with
	// the bucket having the specified key.
	Flush func(key uint64, messages []interface{})
	// Source is the channel from which messages are received.
	Source <-chan interface{}
	// MessageTimeout is the maximum amount of time to wait between messages
	// within a batch. If MessageTimeout elapses after a message is received,
	// but before another message is received, then the current batch is
	// flushed.
	MessageTimeout time.Duration
	// BatchTimeout is the maximum amount of time to wait before flushing a
	// batch. If BatchTimeout elapses after the current batch receives its
	// first message, then the current batch is flushed.
	BatchTimeout time.Duration
	// MaxBatchSize is the maximum number of messages that will be placed in a
	// batch before the batch is flushed. If MaxBatchSize is zero, then there
	// is no maximum.
	MaxBatchSize int
}

// Consume receives messages from a channel, deals the messages into buckets,
// and periodically flushes the buckets. Consume returns when the channel is
// closed, or when ctx is done.
func Consume(ctx context.Context, config Config) {
	// batches maps bucket key to the current batch for that bucket.
	batches := make(map[uint64]*bucket)
	// timer manages a channel on which we wait for timeouts.
	var timer *time.Timer
	// deadlines is a priority queue of upcoming timeout events (deadlines)
	var deadlines deadlineHeap
	// timeout is the channel managed by timer, or nil if there are no upcoming
	// deadlines.
	var timeout <-chan time.Time
	// pool stores *deadline objects for reuse.
	var pool deadlinePool

	for {
		// If we have deadlines but no timeout is set, set the timeout channel
		// appropriately.
		if len(deadlines) != 0 && timeout == nil {
			duration := time.Until(deadlines[0].When)
			if timer == nil {
				timer = time.NewTimer(duration)
			} else {
				timer.Reset(duration)
			}
			timeout = timer.C
		}

		select {
		case <-timeout:
			handleTimeout(&deadlines, batches, config.Flush, &pool)

			// By setting timeout to nil, we signal to the outer loop that the
			// timeout should be reset. The code is at the top of the loop
			// instead of here, because it also needs to happen after the
			// initial message receive.
			timeout = nil
		case message, ok := <-config.Source:
			if !ok {
				return // config.Source is closed
			}
			handleMessage(message, &config, &deadlines, batches, &pool)
		case <-ctx.Done():
			return
		}
	}
}

// handleMessage is the factored out bulk of the "<-config.Source" case in the
// "select" statement within Consume.
func handleMessage(
	message interface{},
	config *Config,
	deadlines *deadlineHeap,
	batches map[uint64]*bucket,
	pool *deadlinePool) {
	// Calculate the reference point for the next deadline immediately,
	// so as not to lose time in the following calculations.
	now := time.Now()

	key := config.Key(message)
	batch := batches[key]
	if batch == nil {
		batch = &bucket{}
		batches[key] = batch
	}
	batch.Messages = append(batch.Messages, message)

	if config.MaxBatchSize != 0 && len(batch.Messages) > config.MaxBatchSize {
		config.Flush(key, batch.Messages)
		batch.Reset()
		return
	}

	if batch.MessageDeadline != nil {
		batch.MessageDeadline.What = defunct
	}
	batch.MessageDeadline = pool.Alloc()
	batch.MessageDeadline.When = now.Add(config.MessageTimeout)
	batch.MessageDeadline.What = messageTimeoutExpired
	batch.MessageDeadline.Key = key
	heap.Push(deadlines, batch.MessageDeadline)

	// If this is the first message in the batch, set a batch deadline.
	if len(batch.Messages) != 1 {
		return
	}
	// Note: We know that batch.BatchDeadline is nil, because the batch
	// was recently empty.
	batch.BatchDeadline = pool.Alloc()
	batch.BatchDeadline.When = now.Add(config.BatchTimeout)
	batch.BatchDeadline.What = batchTimeoutExpired
	batch.BatchDeadline.Key = key
	heap.Push(deadlines, batch.BatchDeadline)
}

// handleTimeout is the factored out bulk of the "<-timeout" case in the
// "select" statement within Consume.
func handleTimeout(
	deadlines *deadlineHeap,
	batches map[uint64]*bucket,
	flush func(uint64, []interface{}),
	pool *deadlinePool) {
	// Process all of the deadlines that are "ready," i.e. in the past
	// or defunct.
	now := time.Now()
	deadlineReady := func() bool {
		if deadlines == nil || len(*deadlines) == 0 {
			return false
		}
		closest := (*deadlines)[0]
		// The deadline is not in the future, or it's defunct.
		return !now.Before(closest.When) || closest.What == defunct
	}

	for ; deadlineReady(); now = time.Now() {
		passed := heap.Pop(deadlines).(*deadline)
		pool.Free(passed)
		if passed.What == defunct {
			continue
		}

		key := passed.Key
		batch := batches[key]
		flush(key, batch.Messages)
		batch.Reset()
	}
}

// reason enumerates the types of deadlines
type reason int

const (
	// defunct means that this deadline is no longer valid (discard it)
	defunct = reason(iota)
	// messageTimeoutExpired means that the batch's _message_ timeout was reached
	messageTimeoutExpired
	// batchTimeoutExpired means that the batch's _batch_ timeout was reached
	batchTimeoutExpired
)

// deadline describes a future event, such as a batch needing to be flushed.
type deadline struct {
	// When is the time at which this deadline is reached (expires).
	When time.Time
	// What is the type of deadline, i.e. what it's for.
	What reason
	// Key is the key of the bucket to which this deadline applies.
	Key uint64
}

// deadlineHeap is a min-heap of deadline, where a deadline A is less than
// another deadline B when A.When precedes B.When.
type deadlineHeap []*deadline

// The following five methods of deadlineHeap implement the
// container/heap.Interface interface (which also happens to implement the
// sort.Interface interface).

func (h deadlineHeap) Len() int           { return len(h) }
func (h deadlineHeap) Less(i, j int) bool { return h[i].When.Before(h[j].When) }
func (h deadlineHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *deadlineHeap) Push(x interface{}) {
	*h = append(*h, x.(*deadline))
}

func (h *deadlineHeap) Pop() interface{} {
	old := *h
	n := len(old)
	last := old[n-1]
	*h = old[0 : n-1]
	return last
}

// bucket is a sequence of messages all having the same key. When the bucket
// (batch) is full, or when one of its timeouts is reached, its messages are
// flushed and removed from the bucket.
type bucket struct {
	Messages        []interface{}
	MessageDeadline *deadline
	BatchDeadline   *deadline
}

func (b *bucket) Reset() {
	if b.MessageDeadline != nil {
		b.MessageDeadline.What = defunct
		b.MessageDeadline = nil
	}
	if b.BatchDeadline != nil {
		b.BatchDeadline.What = defunct
		b.BatchDeadline = nil
	}
	b.Messages = nil
}

// deadlinePool is a pool of *deadline objects. This might be a premature
// optimization, but it's easy and the garbage collector works so hard...
type deadlinePool []*deadline

func (pool *deadlinePool) Alloc() *deadline {
	if pool == nil || len(*pool) == 0 {
		return &deadline{}
	}
	n := len(*pool)
	recycled := (*pool)[n-1]
	*pool = (*pool)[:n-1]
	return recycled
}

func (pool *deadlinePool) Free(d *deadline) {
	*pool = append(*pool, d)
}
