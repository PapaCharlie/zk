//go:build go1.18
// +build go1.18

package zk

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
)

var treeCacheUnmarshaler = func(_ string, data []byte) (string, error) {
	return string(data), nil
}

func TestTreeCache(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		create := func(path string) {
			_, err := zk.Create(path, []byte(path), 0, nil)
			if err != nil {
				t.Fatalf("Failed to create node %q: %+v", path, err)
			}
		}

		zk.reconnectLatch = make(chan struct{})
		nodes := []string{
			"/root",
			"/root/foo",
			"/root/foo/bar",
			"/root/foo/bar/baz",
			"/root/asd",
		}

		for _, node := range nodes {
			create(node)
		}

		outChan := make(chan Event)
		tc, err := NewTreeCacheWithOpts(zk, "/root", treeCacheUnmarshaler, TreeCacheOpts{outChan: outChan})
		if err != nil {
			t.Fatalf("Failed to create TreeCache: %+v", err)
		}

		var tree map[string]NodeData[string]
		checkTree := func(k string) {
			t.Helper()
			v, ok := tree[k]
			if !ok {
				t.Fatalf("Could not find %q in tree", k)
			}
			if v.Err != nil {
				t.Fatalf("%q had an error: %+v", k, v.Err)
			}
			if k != v.T {
				t.Fatalf("Unexpected data for %q: expected %v, got %v", k, []byte(k), v.Data)
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

		newNode := "/root/foo/bar/foo"
		create(newNode)

		<-outChan

		// Test creates are correctly handled
		obj, _, err := tc.Get(newNode)
		if err != nil {
			t.Fatalf("Get(%q) return an error: %+v", newNode, err)
		}
		if newNode != obj {
			t.Fatalf("Unexpected data for %q: expected %q, got %q", newNode, newNode, obj)
		}

		// Test sets are correctly handled
		const foo = "foo"
		_, err = zk.Set(newNode, []byte(foo), -1)
		if err != nil {
			t.Fatalf("Set(%q) failed: %+v", newNode, err)
		}
		<-outChan
		obj, _, err = tc.Get(newNode)
		if err != nil {
			t.Fatalf("Get(%q) return an error: %+v", newNode, err)
		}
		if foo != obj {
			t.Fatalf("Unexpected data for %q: expected %q, got %q", newNode, foo, obj)
		}

		// Test deletes are correctly handled
		err = zk.Delete(newNode, -1)
		if err != nil {
			t.Fatalf("Delete(%q) failed: %+v", newNode, err)
		}
		<-outChan

		_, _, err = tc.Get(newNode)
		if err != ErrNoNode {
			t.Fatalf("Get(%q) should have returned %+v, got %+v", newNode, ErrNoNode, err)
		}

		zk.conn.Close()

		zk2, _, err := ts.ConnectAll()
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}

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
		tc, err := NewTreeCacheWithOpts(zk, root, treeCacheUnmarshaler, TreeCacheOpts{
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
		tc, err := NewTreeCacheWithOpts(zk, "/root", treeCacheUnmarshaler, TreeCacheOpts{MaxRelativeDepth: 5, outChan: outChan})
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

func TestTreeCacheMinDepth(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		create := func(path string) {
			_, err := zk.Create(path, nil, 0, nil)
			requireNoErrorf(t, err, "could not create %q", path)
		}

		const (
			root          = "/root"
			foo           = root + "/foo"
			bar           = root + "/bar"
			expectedPaths = 10
		)

		create(root)
		create(foo)
		for i := 0; i < expectedPaths; i++ {
			path := fmt.Sprintf(foo+"/%d", i)
			create(path)
		}

		outChan := make(chan Event)
		tc, err := NewTreeCacheWithOpts(zk, root, treeCacheUnmarshaler, TreeCacheOpts{MinRelativeDepth: 3, outChan: outChan})
		requireNoErrorf(t, err, "could not create TreeCache")

		children := tc.Children(root)
		if children[root].Err != ErrAboveMinDepth {
			t.Fatalf("Children did not return an error for path above min depth (%+v)", children)
		}

		children = tc.Children(foo)
		if len(children) != len(children) {
			t.Fatalf("Unexpected number of nodes returned by Children (expected %d, got %d)", expectedPaths, len(children))
		}

		tree := tc.Tree(root)
		if len(tree) != len(children) {
			t.Fatalf("Unexpected number of nodes returned by Tree (expected %d, got %d)", expectedPaths, len(tree))
		}

		for i := 0; i < expectedPaths; i++ {
			check := func(path string, m map[string]NodeData[string]) {
				v, ok := m[path]
				if !ok {
					t.Fatalf("%v did not contain %q", m, path)
				}
				requireNoErrorf(t, v.Err, "%q has an error", path)
			}
			path := fmt.Sprintf("%d", i)
			check(path, children)
			check(foo+"/"+path, tree)
		}

		create(bar)
		<-outChan
		_, _, err = tc.Get(bar)
		if err != ErrAboveMinDepth {
			t.Fatalf("TreeCache did not respect min depth for %q", bar)
		}

		const baz = bar + "/baz"
		create(baz)
		<-outChan
		_, _, err = tc.Get(baz)
		requireNoErrorf(t, err, "could not get %q", baz)
	})
}

func TestTreeCache_depthCheck(t *testing.T) {
	tc := &TreeCache[any]{RootPath: "/root"}

	for p, pd := tc.RootPath, 1; pd < 3; p, pd = fmt.Sprintf("%s/%d", p, pd+1), pd+1 {
		if actual := tc.relativeDepth(p); actual != pd {
			t.Fatalf("relativeDepth(%q) returned %d, expected %d", p, actual, pd)
		}

		for depth := 0; depth < 3; depth++ {
			tc.maxRelativeDepth = depth
			tc.minRelativeDepth = depth

			var isAboveMin, isBelowMax bool
			if depth == 0 {
				// If the depth is 0, it disables the feature so both checks should return false
				isAboveMin = false
				isBelowMax = false
			} else {
				isAboveMin = pd < depth
				isBelowMax = pd > depth
			}

			if actual := tc.checkAboveMinDepth(p); actual != isAboveMin {
				t.Fatalf("checkAboveMinDepth(%q) != %v for minRelativeDepth=%d", p, isAboveMin, depth)
			}

			if actual := tc.checkBelowMaxDepth(p); actual != isBelowMax {
				t.Fatalf("checkBelowMaxDepth(%q) != %v for maxRelativeDepth=%d", p, isBelowMax, depth)
			}

		}
	}
}

func TestTreeCacheEmptyDepthSelection(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		_, err := NewTreeCacheWithOpts(zk, "/", treeCacheUnmarshaler, TreeCacheOpts{MinRelativeDepth: 3, MaxRelativeDepth: 2})
		if err == nil {
			t.Fatalf("empty depth selection for treeCache did not return an error")
		}
	})
}

func TestTreeCacheWithInvalidData(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		root := "/root"
		outChan := make(chan Event)
		badNumErr := errors.New("bad number")

		tc, err := NewTreeCacheWithOpts(zk, "/root", func(_ string, data []byte) (int, error) {
			i, err := strconv.Atoi(string(data))
			if err != nil {
				// return badNumErr so it's easier to check that an error from the unmarshaler was returned
				return 0, badNumErr
			} else {
				return i, nil
			}
		}, TreeCacheOpts{outChan: outChan})
		requireNoErrorf(t, err, "could not create int tree cache")

		_, _, err = tc.Get(root)
		if err != ErrNoNode {
			t.Fatalf("Get on misssing node did not return an error")
		}

		const fortyTwo = 42
		expectedData := []byte(strconv.Itoa(fortyTwo))
		_, err = zk.Create(root, expectedData, 0, nil)
		requireNoErrorf(t, err, "create failed")

		<-outChan

		i, data, err := tc.Get(root)
		requireNoErrorf(t, err, "Get returned an error")
		if !bytes.Equal(data, expectedData) {
			t.Fatalf("Get returned unexpected data (expected %v): %v", expectedData, data)
		}
		if i != fortyTwo {
			t.Fatalf("Get returned unexpected value (expected %d): %d", fortyTwo, i)
		}

		invalidInt := []byte("asd")
		_, err = zk.Set(root, invalidInt, -1)
		requireNoErrorf(t, err, "create failed")

		<-outChan

		i, data, err = tc.Get(root)
		if err != badNumErr {
			t.Fatalf("Unmarshaller did not return error")
		}
		if !bytes.Equal(data, invalidInt) {
			t.Fatalf("Get returned unexpected data (expected %v): %v", expectedData, data)
		}
		// i is still expected to be 42 since it's the last known good version of the data
		if i != fortyTwo {
			t.Fatalf("Get returned unexpected value (expected %d): %d", fortyTwo, i)
		}
	})
}
