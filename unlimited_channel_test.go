package zk

import (
	"fmt"
	"reflect"
	"testing"
)

func newEvent(i int) Event {
	return Event{Path: fmt.Sprintf("/%d", i)}
}

func TestQueue(t *testing.T) {
	in := make(chan Event)
	out := toUnlimitedChannel(in)

	// check that events can be pushed without consumers
	for i := 0; i < 100; i++ {
		in <- newEvent(i)
	}

	events := 0
	for actual := range out {
		expected := newEvent(events)
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("Did not receive expected event from queue: actual %+v expected %+v", actual, expected)
		}
		events++
		if events == 100 {
			close(in)
		}
	}
	if events != 100 {
		t.Fatalf("Did not receive 100 events")
	}
}

func TestQueueClose(t *testing.T) {
	in := make(chan Event)
	out := toUnlimitedChannel(in)

	in <- Event{}
	close(in)

	_, ok := <-out
	if !ok {
		t.Fatalf("Closed inifinite queue did not drain remamining events")
	}

	_, ok = <-out
	if ok {
		t.Fatalf("Too many events returned by closed queue")
	}
}
