//go:build go1.18
// +build go1.18

package zk

import (
	"sync"
)

type EventQueue interface {
	Next() <-chan Event
	push(e Event)
	close()
}

type chanEventQueue chan Event

func (c chanEventQueue) Next() <-chan Event {
	return c
}

func (c chanEventQueue) push(e Event) {
	c <- e
}

func (c chanEventQueue) close() {
	close(c)
}

func newChanEventChannel() chanEventQueue {
	return make(chan Event, 1)
}

func newUnlimitedChannelNode() *unlimitedEventQueueNode {
	return &unlimitedEventQueueNode{event: make(chan Event, 1)}
}

type unlimitedEventQueueNode struct {
	event chan Event
	next  *unlimitedEventQueueNode
}

type unlimitedEventQueue struct {
	lock sync.Mutex
	head *unlimitedEventQueueNode
	tail *unlimitedEventQueueNode
}

// newUnlimitedEventQueue uses a backing unlimitedEventQueue used to effectively turn a buffered channel into a channel
// with an infinite buffer by storing all incoming elements into a singly-linked queue and popping them as they are
// read.
func newUnlimitedEventQueue() *unlimitedEventQueue {
	head := newUnlimitedChannelNode()
	q := &unlimitedEventQueue{
		head: head,
		tail: head,
	}
	return q
}

func (q *unlimitedEventQueue) push(e Event) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.tail == nil {
		// Panic like a closed channel
		panic("send on closed unlimited channel")
	}

	node := newUnlimitedChannelNode()
	tail := q.tail
	tail.next = node
	q.tail = node
	tail.event <- e
}

func (q *unlimitedEventQueue) close() {
	q.lock.Lock()
	defer q.lock.Unlock()
	close(q.tail.event)
	q.tail = nil
}

var closedChannel = func() <-chan Event {
	ch := make(chan Event)
	close(ch)
	return ch
}()

func (q *unlimitedEventQueue) Next() <-chan Event {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.head == nil && q.tail == nil {
		return closedChannel
	}

	node := q.head
	q.head = q.head.next
	return node.event
}
