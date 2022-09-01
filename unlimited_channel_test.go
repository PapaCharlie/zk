//go:build go1.18

package zk

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func newEvent(i int) Event {
	return Event{Path: fmt.Sprintf("/%d", i)}
}

func TestUnlimitedChannel(t *testing.T) {
	names := []string{"notClosedAfterPushes", "closeAfterPushes"}
	for i, closeAfterPushes := range []bool{false, true} {
		t.Run(names[i], func(t *testing.T) {
			ch := newUnlimitedEventQueue()
			const eventCount = 2

			// check that events can be pushed without consumers
			for i := 0; i < eventCount; i++ {
				ch.push(newEvent(i))
			}
			if closeAfterPushes {
				ch.close()
			}

			events := 0
			for {
				actual, ok := <-ch.Next()
				expected := newEvent(events)
				if !reflect.DeepEqual(actual, expected) {
					t.Fatalf("Did not receive expected event from queue (ok: %+v): actual %+v expected %+v",
						ok, actual, expected)
				}
				events++
				if events == eventCount {
					if closeAfterPushes {
						select {
						case _, ok := <-ch.Next():
							if ok {
								t.Fatal("Next did not return closed channel")
							}
						case <-time.After(time.Second):
							t.Fatal("Next never closed")
						}
					} else {
						select {
						case e, ok := <-ch.Next():
							t.Fatalf("Next received unexpected value (%+v) or was closed (%+v)", e, ok)
						case <-time.After(time.Millisecond * 10):
							return
						}
					}
					break
				}
			}
		})
	}
}
