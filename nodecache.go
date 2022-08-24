package zk

import (
	"sync"
)

type NodeCacheOpts struct {
	// The retry policy to use when fetching nodes' data and children. If nil, uses DefaultWatcherRetryPolicy
	RetryPolicy RetryPolicy
	// used for debugging, receives events after they are processed
	outChan chan Event
}

// A NodeCache attempts to stay up-to-date with a given node's state using a persistent watch.
type NodeCache struct {
	Path string
	Conn *Conn

	retryPolicy RetryPolicy
	lock        sync.RWMutex
	data        []byte
	events      <-chan Event
	err         error
	stopChan    chan struct{}
	outChan     chan Event
}

func NewNodeCache(conn *Conn, nodePath string) (nc *NodeCache, err error) {
	return NewNodeCacheWithOpts(conn, nodePath, NodeCacheOpts{})
}

func NewNodeCacheWithOpts(conn *Conn, nodePath string, opts NodeCacheOpts) (nc *NodeCache, err error) {
	nc = &NodeCache{
		Path:        nodePath,
		retryPolicy: opts.RetryPolicy,
		Conn:        conn,
		stopChan:    make(chan struct{}),
		outChan:     opts.outChan,
	}
	if nc.retryPolicy == nil {
		nc.retryPolicy = DefaultWatcherRetryPolicy
	}

	nc.events, err = conn.AddPersistentWatch(nodePath, AddWatchModePersistent)
	if err != nil {
		return nil, err
	}

	nc.start()

	return nc, nil
}

// Stop removes the persistent watch that was created for this node.
func (nc *NodeCache) Stop() (err error) {
	err = nc.Conn.RemovePersistentWatch(nc.Path, nc.events)
	if err != nil {
		close(nc.stopChan)
	}
	return err
}

// Get returns the most up-to-date data it has available. Returns zk.ErrNoWatcher if Stop has been called.
func (nc *NodeCache) Get() ([]byte, error) {
	nc.lock.RLock()
	defer nc.lock.RUnlock()
	return nc.data, nc.err
}

// Refresh forces a refresh of the node's data.
func (nc *NodeCache) Refresh() ([]byte, error) {
	data, err := getNodeData(nc.retryPolicy, nc.stopChan, nc.Path, nc.Conn)
	nc.lock.Lock()
	defer nc.lock.Unlock()
	nc.data, nc.err = data, err
	return nc.data, nc.err
}

func (nc *NodeCache) start() {
	ch := toUnlimitedChannel(nc.events)

	// Initial data fetch blocks startup to ensure a sane first read
	nc.Refresh()

	go func() {
		for e := range ch {
			switch e.Type {
			case EventNodeCreated, EventNodeDataChanged, EventWatching:
				nc.Refresh()
			case EventNodeDeleted:
				nc.lock.Lock()
				nc.data, nc.err = nil, ErrNoNode
				nc.lock.Unlock()
			case EventNotWatching:
				// EventNotWatching means that channel's about to close, and we can report the error that caused the
				// closure. We don't zero out the data and stat to reflect the last known state of the node.
				nc.lock.Lock()
				nc.err = e.Err
				nc.lock.Unlock()
			}
			if nc.outChan != nil {
				nc.outChan <- e
			}
		}
		if nc.outChan != nil {
			close(nc.outChan)
		}
	}()
}
