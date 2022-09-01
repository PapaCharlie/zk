//go:build go1.18
// +build go1.18

package zk

import (
	"sync"
)

type unlimitedChannelNode[T any] struct {
	event T
	next  *unlimitedChannelNode[T]
}

type unlimitedChannel[T any] struct {
	head   *unlimitedChannelNode[T]
	tail   *unlimitedChannelNode[T]
	cond   *sync.Cond
	closed bool
}

// ToUnlimitedChannel uses a backing unlimitedChannel used to effectively turn a buffered channel into a channel with an
// infinite buffer by storing all incoming elements into a singly-linked queue and popping them as they are read.
func ToUnlimitedChannel[T any](in <-chan T) <-chan T {
	q := &unlimitedChannel[T]{cond: sync.NewCond(new(sync.Mutex))}

	go func() {
		defer q.close()
		for e := range in {
			q.push(e)
		}
	}()

	out := make(chan T)
	go func() {
		for {
			e, closed := q.next()
			if closed {
				close(out)
				return
			}
			out <- e
		}
	}()

	return out
}

func (q *unlimitedChannel[T]) push(e T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.closed {
		// Panic like a closed channel
		panic("send on closed unlimited channel")
	}

	if q.head == nil {
		q.head = &unlimitedChannelNode[T]{event: e}
		q.tail = q.head
	} else {
		q.tail.next = &unlimitedChannelNode[T]{event: e}
		q.tail = q.tail.next
	}
	q.cond.Signal()
}

func (q *unlimitedChannel[T]) close() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.closed = true
	q.cond.Signal()
}

func (q *unlimitedChannel[T]) next() (T, bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	// Wait until the queue either has an element or has been closed
	for q.head == nil && !q.closed {
		q.cond.Wait()
	}

	if q.head != nil {
		e := q.head.event
		q.head = q.head.next
		return e, false
	} else { // we know from the condition check above that if the head is nil, then the queue is closed
		var t T
		return t, true
	}
}
