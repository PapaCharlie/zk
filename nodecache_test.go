//go:build go1.18
// +build go1.18

package zk

import (
	"testing"
	"time"
)

var nodeCacheUnmarshaler = func(data []byte) (string, error) {
	return string(data), nil
}

func TestNodeCache(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		zk.reconnectLatch = make(chan struct{})

		outChan := make(chan Event)
		nc, err := NewNodeCacheWithOpts[string](zk, "/foo", nodeCacheUnmarshaler, NodeCacheOpts[string]{outChan: outChan})
		requireNoErrorf(t, err, "Failed to initialize node cache")

		_, _, err = nc.Get()
		if err != ErrNoNode {
			t.Fatalf("Get did not return zk.ErrNoNode: %+v", err)
		}

		testData := "foo"
		_, expectedStat, err := zk.CreateAndReturnStat(nc.Path, []byte(testData), 0, nil)
		requireNoErrorf(t, err, "Failed to set data for %q", nc.Path)

		select {
		case e := <-outChan:
			if e.Type != EventNodeCreated {
				t.Fatalf("Unexpected event: %+v", e)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Did not get create event")
		}

		data, stat, err := nc.Get()
		if err != nil {
			t.Fatalf("Get returned an error: %+v", err)
		}
		if data != testData {
			t.Fatalf("Get did not return the correct data, expected %q, got %q", testData, data)
		}
		if stat != *expectedStat {
			t.Fatalf("Get did not return the correct stat, expected %q, got %q", expectedStat, stat)
		}

		err = zk.Delete(nc.Path, -1)
		requireNoErrorf(t, err, "Failed to delete %q", nc.Path)

		select {
		case e := <-outChan:
			if e.Type != EventNodeDeleted {
				t.Fatalf("Unexpected event: %+v", e)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Did not get create event")
		}

		_, _, err = nc.Get()
		if err != ErrNoNode {
			t.Fatalf("Get returned a node after a delete")
		}

		zk.conn.Close()

		zk2, _, err := ts.ConnectAll()
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}

		_, expectedStat, err = zk2.CreateAndReturnStat(nc.Path, []byte(testData), 0, nil)
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}

		close(zk.reconnectLatch)

		select {
		case e := <-outChan:
			if e.Type != EventWatching {
				t.Fatalf("Unexpected event: %+v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get reconnect event")
		}

		data, stat, err = nc.Get()
		if err != nil {
			t.Fatalf("Get returned an error: %+v", err)
		}
		if data != testData {
			t.Fatalf("Get did not return the correct data, expected %q, got %q", testData, data)
		}
		if stat != *expectedStat {
			t.Fatalf("Get did not return the correct stat, expected %q, got %q", expectedStat, stat)
		}
	})
}

func TestNodeCacheStartup(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		testData := "foo"

		const path = "/foo"
		_, expectedStat, err := zk.CreateAndReturnStat(path, []byte(testData), 0, nil)
		requireNoErrorf(t, err, "Failed to create %q", path)

		nc, err := NewNodeCache[string](zk, path, nodeCacheUnmarshaler)
		requireNoErrorf(t, err, "Failed to initialize node cache")

		data, stat, err := nc.Get()
		if err != nil {
			t.Fatalf("Get returned an error: %+v", err)
		}
		if data != testData {
			t.Fatalf("Get did not return the correct data, expected %q, got %q", testData, data)
		}
		if stat != *expectedStat {
			t.Fatalf("Get did not return the correct stat, expected %q, got %q", expectedStat, stat)
		}
	})
}
