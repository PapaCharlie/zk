//go:build go1.18
// +build go1.18

package zk

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
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
		expectedStats := map[string]Stat{}
		create := func(path string) {
			_, stat, err := zk.CreateAndReturnStat(path, []byte(path), 0, nil)
			if err != nil {
				t.Fatalf("Failed to create node %q: %+v", path, err)
			}
			expectedStats[path] = *stat
		}
		set := func(path, data string) {
			stat, err := zk.Set(path, []byte(data), -1)
			if err != nil {
				t.Fatalf("Failed to set node %q: %+v", path, err)
			}
			expectedStats[path] = *stat
		}
		checkStat := func(path string, stat Stat) {
			t.Helper()
			if expected := expectedStats[path]; stat.Mzxid != expected.Mzxid {
				t.Fatalf("Unexpected stat for %q: expected %+v, got %+v", path, expected, stat)
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
		tc, err := NewTreeCacheWithOpts(zk, "/root", treeCacheUnmarshaler, TreeCacheOpts[string]{outChan: outChan})
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
			checkStat(k, v.Stat)
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
		obj, _, stat, err := tc.Get(newNode)
		if err != nil {
			t.Fatalf("Get(%q) return an error: %+v", newNode, err)
		}
		if newNode != obj {
			t.Fatalf("Unexpected data for %q: expected %q, got %q", newNode, newNode, obj)
		}
		checkStat(newNode, stat)

		// Test sets are correctly handled
		const foo = "foo"
		set(newNode, foo)
		<-outChan
		obj, _, stat, err = tc.Get(newNode)
		if err != nil {
			t.Fatalf("Get(%q) return an error: %+v", newNode, err)
		}
		if foo != obj {
			t.Fatalf("Unexpected data for %q: expected %q, got %q", newNode, foo, obj)
		}
		checkStat(newNode, stat)

		// Test deletes are correctly handled
		err = zk.Delete(newNode, -1)
		if err != nil {
			t.Fatalf("Delete(%q) failed: %+v", newNode, err)
		}
		<-outChan

		_, _, _, err = tc.Get(newNode)
		if err != ErrNoNode {
			t.Fatalf("Get(%q) should have returned %+v, got %+v", newNode, ErrNoNode, err)
		}

		zk.conn.Close()

		zk2, _, err := ts.ConnectAll()
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}

		_, newStat, err := zk2.CreateAndReturnStat(newNode, []byte(newNode), 0, nil)
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}
		expectedStats[newNode] = *newStat

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
		checkStat("/root/foo/bar", tree["bar"].Stat)

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
		tc, err := NewTreeCacheWithOpts(zk, root, treeCacheUnmarshaler, TreeCacheOpts[string]{
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

var badNumErr = errors.New("bad number")
var intUnmarshaler = func(path string, data []byte) (int, error) {
	i, err := strconv.Atoi(string(data))
	if err != nil {
		// return badNumErr so it's easier to check that an error from the unmarshaler was returned
		return 0, badNumErr
	} else {
		return i, nil
	}
}

func TestTreeCacheWithInvalidData(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		root := "/root"
		outChan := make(chan Event)

		tc, err := NewTreeCacheWithOpts(zk, "/root", intUnmarshaler, TreeCacheOpts[int]{outChan: outChan})
		requireNoErrorf(t, err, "could not create int tree cache")

		_, _, _, err = tc.Get(root)
		if err != ErrNoNode {
			t.Fatalf("Get on misssing node did not return an error")
		}

		const fortyTwo = 42
		expectedData := []byte(strconv.Itoa(fortyTwo))
		_, expectedStat, err := zk.CreateAndReturnStat(root, expectedData, 0, nil)
		requireNoErrorf(t, err, "create failed")

		<-outChan

		i, data, stat, err := tc.Get(root)
		requireNoErrorf(t, err, "Get returned an error")
		if !bytes.Equal(data, expectedData) {
			t.Fatalf("Get returned unexpected data (expected %v): %v", expectedData, data)
		}
		if i != fortyTwo {
			t.Fatalf("Get returned unexpected value (expected %d): %d", fortyTwo, i)
		}
		if stat != *expectedStat {
			t.Fatalf("Unexpected stat for %q: expected %+v, got %+v", root, *expectedStat, stat)
		}

		invalidInt := []byte("asd")
		expectedStat, err = zk.Set(root, invalidInt, -1)
		requireNoErrorf(t, err, "set failed")

		<-outChan

		i, data, stat, err = tc.Get(root)
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
		if stat != *expectedStat {
			t.Fatalf("Unexpected stat for %q: expected %+v, got %+v", root, *expectedStat, stat)
		}
	})
}

type mockTreeCacheWatcher[T any] struct {
	onCreateOrUpdate       func(path string, data NodeData[T])
	onDelete               func(path string)
	onBootstrapOrReconnect func(tree map[string]NodeData[T])
}

func (m *mockTreeCacheWatcher[T]) OnCreateOrUpdate(path string, data NodeData[T]) {
	m.onCreateOrUpdate(path, data)
}

func (m *mockTreeCacheWatcher[T]) OnDelete(path string) {
	m.onDelete(path)
}

func (m *mockTreeCacheWatcher[T]) OnBootstrapOrReconnect(tree map[string]NodeData[T]) {
	m.onBootstrapOrReconnect(tree)
}

func TestTreeCacheWatcherAndFilter(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		const (
			root    = "/root"
			foo     = root + "/foo"
			bar     = root + "/bar"
			baz     = root + "/baz"
			qux     = root + "/qux"
			ignored = root + "/ignored"
		)
		expectedData := map[string]NodeData[int]{}
		createOrSetNode := func(zk *Conn, path string, data int) {
			dataBytes := []byte(fmt.Sprint(data))
			_, stat, err := zk.CreateAndReturnStat(path, dataBytes, 0, nil)
			if err == ErrNodeExists {
				stat, err = zk.Set(path, dataBytes, -1)
			}
			if err != nil {
				t.Fatalf("Failed to create/set node %q: %+v", path, err)
			}
			if path != ignored {
				expectedData[path] = NodeData[int]{
					T:    data,
					Data: dataBytes,
					Stat: *stat,
				}
			}
		}
		deleteNode := func(zk *Conn, path string) {
			err := zk.Delete(path, -1)
			if err != nil {
				t.Fatalf("Failed to delete node %q: %+v", path, err)
			}
			delete(expectedData, path)
		}

		zk.reconnectLatch = make(chan struct{})
		_, _, err := zk.CreateAndReturnStat(root, nil, 0, nil)
		if err != nil {
			t.Fatalf("Failed to create %q: %+v", root, err)
		}

		createOrSetNode(zk, foo, 1)
		createOrSetNode(zk, bar, 2)
		createOrSetNode(zk, ignored, -1)

		watchedData := map[string]NodeData[int]{}
		watcher := &mockTreeCacheWatcher[int]{
			onCreateOrUpdate: func(path string, data NodeData[int]) {
				if path == ignored {
					t.Fatalf("Received watch callback for %q!", ignored)
				}
				watchedData[path] = data
			},
			onDelete: func(path string) {
				if path == ignored {
					t.Fatalf("Received watch callback for %q!", ignored)
				}
				delete(watchedData, path)
			},
			onBootstrapOrReconnect: func(tree map[string]NodeData[int]) {
				if _, ok := tree[ignored]; ok {
					t.Fatalf("Received watch callback for %q!", ignored)
				}
				watchedData = tree
			},
		}

		outChan := make(chan Event)
		_, err = NewTreeCacheWithOpts(zk, root, intUnmarshaler, TreeCacheOpts[int]{
			outChan: outChan,
			Watcher: watcher,
			Filter: func(nodePath string) (getData, getChildren bool) {
				switch nodePath {
				case root:
					return false, true
				case ignored:
					return false, false
				default:
					return true, true
				}
			},
		})
		if err != nil {
			t.Fatalf("Failed to create TreeCache: %+v", err)
		}

		requireEqual := func(op string, expected, actual any) {
			if !reflect.DeepEqual(expected, actual) {
				t.Fatalf("Watcher %s did not yield expected results.\nExpected: %+v\nActual:   %+v",
					op, expected, actual)
			}
		}

		requireEqual("bootstrap", expectedData, watchedData)

		createOrSetNode(zk, foo, 42)
		<-outChan
		requireEqual("update", expectedData[foo], watchedData[foo])

		createOrSetNode(zk, baz, 27)
		<-outChan
		requireEqual("create", expectedData[baz], watchedData[baz])

		deleteNode(zk, baz)
		<-outChan
		if _, ok := watchedData[baz]; ok {
			t.Fatalf("Watcher delete did not fire!")
		}

		zk.conn.Close()

		zk2, _, err := ts.ConnectAll()
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}
		createOrSetNode(zk2, qux, 5)
		deleteNode(zk2, bar)

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

		requireEqual("boostrap", expectedData, watchedData)
	})
}

