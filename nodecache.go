//go:build go1.18
// +build go1.18

package zk

type NodeCacheOpts struct {
	// The retry policy to use when fetching nodes' data and children. If nil, uses DefaultWatcherRetryPolicy
	RetryPolicy RetryPolicy
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
	return NewNodeCacheWithOpts[T](conn, nodePath, unmarshaler, NodeCacheOpts{})
}

func NewNodeCacheWithOpts[T any](
	conn *Conn,
	nodePath string,
	unmarshaler func(data []byte) (T, error),
	opts NodeCacheOpts,
) (nc *NodeCache[T], err error) {
	tc, err := NewTreeCacheWithOpts[T](
		conn,
		nodePath,
		func(path string, data []byte) (T, error) {
			return unmarshaler(data)
		},
		TreeCacheOpts{
			RetryPolicy:      opts.RetryPolicy,
			MinRelativeDepth: 1,
			MaxRelativeDepth: 1,
			outChan:          opts.outChan,
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
func (nc *NodeCache[T]) Get() (T, error) {
	t, _, err := nc.tc.Get(nc.Path)
	return t, err
}

// GetData returns the most up-to-date data it has available. Returns zk.ErrNoWatcher if Stop has been called.
func (nc *NodeCache[T]) GetData() ([]byte, error) {
	_, data, err := nc.tc.Get(nc.Path)
	return data, err
}

// Refresh forces a refresh of the node's data.
func (nc *NodeCache[T]) Refresh() ([]byte, T, error) {
	return nc.tc.Refresh(nc.Path)
}
