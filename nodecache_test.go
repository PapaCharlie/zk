package zk

import (
	"bytes"
	"testing"
	"time"
)

func TestNodeCache(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		zk.reconnectLatch = make(chan struct{})

		outChan := make(chan Event)
		nc, err := NewNodeCacheWithOpts(zk, "/foo", NodeCacheOpts{outChan: outChan})
		requireNoErrorf(t, err, "Failed to initialize node cache")

		_, err = nc.Get()
		if err != ErrNoNode {
			t.Fatalf("Get did not return zk.ErrNoNode: %+v", err)
		}

		testData := []byte{1, 2, 3, 4}
		_, err = zk.Create(nc.Path, testData, 0, nil)
		requireNoErrorf(t, err, "Failed to set data for %q", nc.Path)

		select {
		case e := <-outChan:
			if e.Type != EventNodeCreated {
				t.Fatalf("Unexpected event: %+v", e)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Did not get create event")
		}

		data, err := nc.Get()
		if err != nil {
			t.Fatalf("Get returned an error: %+v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Fatalf("Get did not return the correct data, expected %+v, got %+v", testData, data)
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

		_, err = nc.Get()
		if err != ErrNoNode {
			t.Fatalf("Get returned a node after a delete")
		}

		zk.conn.Close()

		zk2, _, err := ts.ConnectAll()
		if err != nil {
			t.Fatalf("create returned an error: %+v", err)
		}

		_, err = zk2.Create(nc.Path, testData, 0, nil)
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

		data, err = nc.Get()
		if err != nil {
			t.Fatalf("Get returned an error: %+v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Fatalf("Get did not return the correct data, expected %+v, got %+v", testData, data)
		}
	})
}

func TestNodeCacheStartup(t *testing.T) {
	RequireMinimumZkVersion(t, "3.6")
	WithTestCluster(t, 10*time.Second, func(ts *TestCluster, zk *Conn) {
		testData := []byte{1, 2, 3, 4}

		const path = "/foo"
		_, err := zk.Create(path, testData, 0, nil)
		requireNoErrorf(t, err, "Failed to create %q", path)

		nc, err := NewNodeCache(zk, path)
		requireNoErrorf(t, err, "Failed to initialize node cache")

		data, err := nc.Get()
		if err != nil {
			t.Fatalf("Get returned an error: %+v", err)
		}
		if !bytes.Equal(data, testData) {
			t.Fatalf("Get did not return the correct data, expected %+v, got %+v", testData, data)
		}
	})
}
