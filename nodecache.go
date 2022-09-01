//go:build go1.18
// +build go1.18

package zk

type NodeCacheWatcher[T any] interface {
	OnCreateOrUpdate(path string, data NodeData[T])
	OnDelete(path string)
}

type treeToNodeCacheWatcherAdapter[T any] struct {
	NodeCacheWatcher[T]
}

func (n *treeToNodeCacheWatcherAdapter[T]) OnBootstrapOrReconnect(tree map[string]NodeData[T]) {
	// The map will by definition only have one element so this loop will only call OnCreateOrUpdate once
	for k, v := range tree {
		n.NodeCacheWatcher.OnCreateOrUpdate(k, v)
	}
}

type NodeCacheOpts[T any] struct {
	// The retry policy to use when fetching nodes' data and children. If nil, uses DefaultWatcherRetryPolicy
	RetryPolicy RetryPolicy
	// If specified, the watcher's corresponding methods will be invoked whenever the state changes
	Watcher NodeCacheWatcher[T]
	// Used to receive a callback when events are processed, for debugging purposes. Ignored if nil.
	outChan chan Event
}

// A NodeCache attempts to stay up-to-date with a given node's state using a persistent watch. See TreeCache for more
// details
type NodeCache[T any] struct {
	Path string
	Conn *Conn

	tc *TreeCache[T]
}

var NodeIdentity = func(data []byte) ([]byte, error) {
	return data, nil
}

func NewNodeCache[T any](
	conn *Conn,
	nodePath string,
	unmarshaler func(data []byte) (T, error),
) (nc *NodeCache[T], err error) {
	return NewNodeCacheWithOpts[T](conn, nodePath, unmarshaler, NodeCacheOpts[T]{})
}

func NewNodeCacheWithOpts[T any](
	conn *Conn,
	nodePath string,
	unmarshaler func(data []byte) (T, error),
	opts NodeCacheOpts[T],
) (nc *NodeCache[T], err error) {
	var watcher TreeCacheWatcher[T]
	if opts.Watcher != nil {
		watcher = &treeToNodeCacheWatcherAdapter[T]{NodeCacheWatcher: opts.Watcher}
	}
	tc, err := NewTreeCacheWithOpts[T](
		conn,
		nodePath,
		func(path string, data []byte) (T, error) {
			return unmarshaler(data)
		},
		TreeCacheOpts[T]{
			RetryPolicy: opts.RetryPolicy,
			Watcher:     watcher,
			outChan:     opts.outChan,
			isNodeCache: true,
		},
	)
	if err != nil {
		return nil, err
	}

	return &NodeCache[T]{
		Path: tc.RootPath,
		Conn: tc.Conn,
		tc:   tc,
	}, nil
}

// Stop removes the persistent watch that was created for this node.
func (nc *NodeCache[T]) Stop() (err error) {
	return nc.tc.Stop()
}

// Get returns the most up-to-date unmarshalled object it has available. Returns zk.ErrNoWatcher if Stop has been
// called.
func (nc *NodeCache[T]) Get() (T, Stat, error) {
	t, _, stat, err := nc.tc.Get(nc.Path)
	return t, stat, err
}

// GetData returns the most up-to-date data it has available. Returns zk.ErrNoWatcher if Stop has been called.
func (nc *NodeCache[T]) GetData() ([]byte, Stat, error) {
	_, data, stat, err := nc.tc.Get(nc.Path)
	return data, stat, err
}

// TODO: Investigate this to ensure it gets handled properly vis-a-vis the event loop. Until then this functionality is
//  not available
// // Refresh forces a refresh of the node's data.
// func (nc *NodeCache[T]) Refresh() ([]byte, T, Stat, error) {
// 	return nc.tc.Refresh(nc.Path)
// }
