package zk

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestExecuteWithRetries(t *testing.T) {
	errors := make([]error, 5)
	for i := range errors {
		errors[i] = fmt.Errorf("%d", i)
	}
	idx := 0
	call := func() error {
		err := errors[idx]
		idx++
		return err
	}

	stopChan := make(chan struct{})

	err := ExecuteWithRetries(
		func(error) bool { return false },
		func(int) time.Duration { return 0 },
		stopChan,
		call,
	)
	if err != errors[0] {
		t.Fatalf("Did not get correct error: %+v", err)
	}

	idx = 0
	attempts := 0
	err = ExecuteWithRetries(
		func(error) bool {
			return idx <= 2
		},
		func(int) time.Duration {
			attempts++
			return 0
		},
		stopChan,
		call,
	)
	if err != errors[2] {
		t.Fatalf("Did not get correct error: %+v", err)
	}
	if attempts != 2 {
		t.Fatalf("Did not get correct attempt count: %d", attempts)
	}

	idx = 0
	attempts = 0
	err = ExecuteWithRetries(
		func(error) bool {
			return true
		},
		func(int) time.Duration {
			attempts++
			// If call is called again after stopChan is closed, it will panic because no more errors remain
			if attempts == 5 {
				close(stopChan)
			}
			return 0
		},
		stopChan,
		call,
	)
	if err != errors[4] {
		t.Fatalf("Did not get correct error: %+v", err)
	}
}

func TestBootstrap(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		nodes := []string{
			"/root",
			"/root/foo",
			"/root/foo/bar",
			"/root/foo/bar/baz",
			"/root/asd",
		}
		expectedData := map[string]NodeData{}
		for i, n := range nodes {
			data := []byte(strconv.Itoa(i))
			_, stat, err := zk.CreateAndReturnStat(n, data, 0, nil)
			if err != nil {
				t.Fatalf("Failed to create %q: %+v", n, err)
			}
			expectedData[n] = NodeData{
				Data: data,
				Stat: *stat,
			}
		}
		// refresh the stats after all nodes are created to ensure the child counts and Pzxids are all up-to-date
		for _, n := range nodes {
			_, stat, err := zk.Exists(n)
			if err != nil {
				t.Fatalf("Failed to stat %q: %+v", n, err)
			}
			nd := expectedData[n]
			nd.Stat = *stat
			expectedData[n] = nd
		}

		root := Bootstrap(zk, nodes[0], nil, nil, nil, 0, 0)
		if root.Err != nil {
			t.Fatalf("Could not create tree cache: %+v", root.Err)
		}

		actualData := root.ToMap(nodes[0])
		if !reflect.DeepEqual(expectedData, actualData) {
			t.Fatalf("RootTree did not yield expected results.\nExpected: %+v\nActual:   %+v", expectedData, actualData)
		}
	})
}
