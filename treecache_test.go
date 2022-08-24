package zk

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestTreeCache(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		zk.reconnectLatch = make(chan struct{})
		nodes := []string{
			"/root",
			"/root/foo",
			"/root/foo/bar",
			"/root/foo/bar/baz",
			"/root/asd",
		}

		for _, node := range nodes {
			_, err := zk.Create(node, []byte(node), 0, nil)
			if err != nil {
				t.Fatalf("Failed to create node %q: %+v", node, err)
			}
		}

		outChan := make(chan Event)
		tc, err := NewTreeCacheWithOpts(zk, "/root", TreeCacheOpts{outChan: outChan})
		if err != nil {
			t.Fatalf("Failed to create TreeCache: %+v", err)
		}

		var tree map[string]NodeData
		checkTree := func(k string) {
			t.Helper()
			v, ok := tree[k]
			if !ok {
				t.Fatalf("Could not find %q in tree", k)
			}
			if v.Err != nil {
				t.Fatalf("%q had an error: %+v", k, v.Err)
			}
			if k != string(v.Data) {
				t.Fatalf("Unexpected data for %q: expected %v, got %v", k, []byte(k), v.Data)
			}
		}

		tree = tc.Tree("/root")
		if len(nodes) != len(tree) {
			t.Fatalf("Incorrect node count from tree: expected %d got %d (%v)", len(nodes), len(tree), tree)
		}
		for _, node := range nodes {
			checkTree(node)
		}

		tree = tc.Tree("/root/foo/bar")
		if len(tree) != 2 {
			t.Fatalf("Incorrect node count from tree: expected 2 got %d", len(tree))
		}
		checkTree("/root/foo/bar")
		checkTree("/root/foo/bar/baz")

		zk.conn.Close()

		zk2, _, err := ts.ConnectAll()
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}

		newNode := "/root/foo/bar/foo"
		_, err = zk2.Create(newNode, []byte(newNode), 0, nil)
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}

		close(zk.reconnectLatch)
		// wait for reconnect
		select {
		case e := <-outChan:
			if e.Type != EventWatching {
				t.Fatalf("Unexpected event %+v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not reconnect!")
		}

		tree = tc.Tree("/root/foo/bar")
		if len(tree) != 3 {
			t.Fatalf("Incorrect node count from tree: expected 3 got %d (%v)", len(tree), tree)
		}
		checkTree("/root/foo/bar")
		checkTree("/root/foo/bar/baz")
		checkTree(newNode)

		tree = tc.Children("/root/foo")
		if len(tree) != 1 {
			t.Fatalf("Incorrect node count from tree: expected %d got %d (%v)", len(nodes), len(tree), tree)
		}
		if err = tree["bar"].Err; err != nil {
			t.Fatalf("Unexpected error in %q: %+v", "/root/foo/bar", err)
		}
		if data := string(tree["bar"].Data); data != "/root/foo/bar" {
			t.Fatalf("Unexpected data in %q: %q", "/root/foo/bar", data)
		}

		err = tc.Stop()
		if err != nil {
			t.Fatalf("Stop returned an error: %+v", err)
		}

		err = tc.Stop()
		if err != ErrNoWatcher {
			t.Fatalf("Unexpected error returned from Stop: %+v", err)
		}

		select {
		case e := <-outChan:
			if e.Type != EventNotWatching {
				t.Fatalf("Unexpected event %+v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not stop watching!")
		}

		tree = tc.Tree(tc.RootPath)
		if len(nodes)+1 != len(tree) {
			t.Fatalf("Incorrect node count from tree: expected %d got %d (%v)", len(nodes), len(tree), tree)
		}

		for k, v := range tree {
			if v.Err == nil {
				t.Fatalf("No error on %q", k)
			}
		}

		data, children, err := tc.Get(tc.RootPath)
		if err == nil {
			t.Fatalf("Get after stop did not return an error")
		}
		if string(data) != tc.RootPath {
			t.Fatalf("Unepxected data in %q: %q", tc.RootPath, string(data))
		}
		if !reflect.DeepEqual(children, []string{"asd", "foo"}) {
			t.Fatalf("Unepxected children in %q: %q", tc.RootPath, children)
		}
	})
}

func TestTreeCacheStopDuringRetry(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		const root = "/foo"

		var inRetry sync.WaitGroup
		inRetry.Add(1)

		bootStrapped := false

		outChan := make(chan Event)
		tc, err := NewTreeCacheWithOpts(zk, root, TreeCacheOpts{
			RetryPolicy: RetryPolicyFunc(func(attempt int, lastError error) time.Duration {
				// Don't block during bootstrap
				if !bootStrapped {
					bootStrapped = true
					return -1
				} else {
					inRetry.Done()
					return math.MaxInt64
				}
			}),
			outChan: outChan,
		})
		requireNoErrorf(t, err, "Could not create TreeCache: %+v", err)

		_, err = zk.Create(root, nil, 0, nil)
		requireNoErrorf(t, err, "Could not create %q: %+v", root, err)

		// Updating the node then deleting will queue an EventNodeDataChanged then an EventNodeDeleted.
		_, err = zk.Set(root, []byte{1}, -1)
		requireNoErrorf(t, err, "Could not update %q: %+v", root, err)

		err = zk.Delete(root, -1)
		requireNoErrorf(t, err, "Could not delete %q: %+v", root, err)

		// Because the cache is blocked until we read outChan, we know that when it tries to process the queued
		// EventNodeDataChanged, it will return an ErrNoNode, forcing it to execute the RetryPolicy. This policy returns
		// an infinite timeout so this test is now stuck until Stop is called
		select {
		case e := <-outChan:
			if e.Type != EventNodeCreated {
				t.Fatalf("Unexpected event: %+v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get node creation")
		}

		// Wait until the retry loop has started to stop the cache
		inRetry.Wait()
		err = tc.Stop()
		requireNoErrorf(t, err, "Stop returned an unexpected error")

		select {
		case e := <-outChan:
			if e.Type != EventNodeDataChanged {
				t.Fatalf("Unexpected event: %+v", e)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Did not early exit retry loop")
		}
	})
}

func TestTreeCacheMaxDepth(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		nodePath := "/root"

		expectedPaths := map[string]bool{}

		for i := 0; i < 10; i++ {
			_, err := zk.Create(nodePath, nil, 0, nil)
			requireNoErrorf(t, err, "could not create %q", nodePath)
			if i < 5 {
				expectedPaths[nodePath] = true
			}
			nodePath += fmt.Sprintf("/%d", i)
		}

		outChan := make(chan Event)
		tc, err := NewTreeCacheWithOpts(zk, "/root", TreeCacheOpts{MaxDepth: 5, outChan: outChan})
		requireNoErrorf(t, err, "could not create TreeCache")

		tree := tc.Tree("/root")
		for k, v := range tree {
			requireNoErrorf(t, v.Err, "%q has an error", k)
			if !expectedPaths[k] {
				t.Fatalf("TreeCache did not respect max depth: %q", k)
			} else {
				delete(expectedPaths, k)
			}
		}
		if len(expectedPaths) > 0 {
			t.Fatalf("Not all expected paths present in TreeCache: %+v", expectedPaths)
		}

		newPath := "/root/foo"
		_, err = zk.Create(newPath, nil, 0, nil)
		requireNoErrorf(t, err, "could not create %q", newPath)

		t.Log(<-outChan)

		tree = tc.Tree("/root")
		if v, ok := tree[newPath]; !ok || v.Err != nil {
			t.Fatalf("Tree did not contain new node shallower than max depth (got %v)", tree)
		}

		newPath = "/root/0/1/2/3/foo" // this has depth of 6 so it should be ignored
		_, err = zk.Create(newPath, nil, 0, nil)
		requireNoErrorf(t, err, "could not create %q", newPath)

		t.Log(<-outChan)

		tree = tc.Tree("/root")
		if _, ok := tree[newPath]; ok {
			t.Fatalf("Tree did not ignore node deeper than max depth: %q", newPath)
		}
	})
}

func TestTreeCache_isPastMaxDepth(t *testing.T) {
	tests := []struct {
		maxDepth int
		paths    map[string]bool
	}{
		{
			maxDepth: 0,
			paths: map[string]bool{
				"/root":             false,
				"/root/foo":         false,
				"/root/foo/bar":     false,
				"/root/foo/bar/baz": false,
			},
		},
		{
			maxDepth: 1,
			paths: map[string]bool{
				"/root":             false,
				"/root/foo":         true,
				"/root/foo/bar":     true,
				"/root/foo/bar/baz": true,
			},
		},
		{
			maxDepth: 2,
			paths: map[string]bool{
				"/root":             false,
				"/root/foo":         false,
				"/root/foo/bar":     true,
				"/root/foo/bar/baz": true,
			},
		},
	}
	tc := &TreeCache{RootPath: "/root"}
	for _, test := range tests {
		tc.maxDepth = test.maxDepth
		for path, isPast := range test.paths {
			if actual := tc.isPastMaxDepth(path); actual != isPast {
				t.Fatalf("isPastMaxDepth(%q) for maxDepth=%d returned %v, expected %v", path, tc.maxDepth, actual, isPast)
			}
		}
	}
}
