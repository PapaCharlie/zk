//go:build go1.18
// +build go1.18

package zk

import (
	"fmt"
	"strings"
	"sync"
)

type TreeCacheOpts struct {
	// The retry policy to use when fetching nodes' data and children. If nil, uses DefaultWatcherRetryPolicy
	RetryPolicy RetryPolicy
	// Provides a minimum depth limit (inclusive) when bootstrapping and listening to watch events. The root is
	// considered depth 1, so any negative or zero value disables this feature. Nodes below the minimum depth will never
	// be returned by TreeCache.Get, TreeCache.Tree or TreeCache.Children.
	// For example, to listen to the children of the root path without listening to the root itself, or any of the
	// children's children, set both MinRelativeDepth and MaxRelativeDepth to 2.
	MinRelativeDepth int
	// Counterpart to MinRelativeDepth for maximum depth.
	MaxRelativeDepth int
	// Used to receive a callback when events are processed, for debugging purposes. Ignored if nil.
	outChan chan Event
}

// A TreeCache leverages the new persistent watch APIs introduced in 3.6 to keep a local data structure that mirrors
// the remote state as closely as possible. While it's impossible to stay in transactional sync (namely because the
// watches provide neither the transaction id that caused the change, nor the updated data), it's possible to stay
// relatively close. The TreeCache will bootstrap on startup and reboostrap if a connection loss is detected. It also
// provides a mechanism to unmarshal the data of each node into the desired type T. If a node is updated and the
// unmarshaler returns an error, calls to Get, Children and Tree will reflect the up-to-date data but only the most
// recent successfully unmarshaled T, and the unmarshaling error will be returned. In other words, invalid data is
// ignored but still accessible.
type TreeCache[T any] struct {
	RootPath string
	Conn     *Conn

	root             *treeCacheNode[T]
	unmarshaler      func(path string, data []byte) (T, error)
	retryPolicy      RetryPolicy
	minRelativeDepth int
	maxRelativeDepth int

	events   <-chan Event
	stopChan chan struct{}
	outChan  chan Event
}

type treeCacheNode[T any] struct {
	lock     *sync.RWMutex
	data     []byte
	t        T
	children map[string]*treeCacheNode[T]
	err      error
}

// TreeIdentity is a noop unmarshaler that can be pass into the TreeCache constructors for a TreeCache that does not
// deserialize
var TreeIdentity = func(_ string, data []byte) ([]byte, error) {
	return data, nil
}

func NewTreeCache[T any](
	conn *Conn,
	rootPath string,
	unmarshaler func(path string, data []byte) (T, error),
) (tc *TreeCache[T], err error) {
	return NewTreeCacheWithOpts[T](conn, rootPath, unmarshaler, TreeCacheOpts{})
}

func NewTreeCacheWithOpts[T any](
	conn *Conn,
	rootPath string,
	unmarshaler func(path string, data []byte) (T, error),
	opts TreeCacheOpts,
) (tc *TreeCache[T], err error) {
	tc = &TreeCache[T]{
		RootPath:         rootPath,
		root:             newTreeCacheNode[T](),
		unmarshaler:      unmarshaler,
		retryPolicy:      opts.RetryPolicy,
		minRelativeDepth: opts.MinRelativeDepth,
		maxRelativeDepth: opts.MaxRelativeDepth,
		Conn:             conn,
		stopChan:         make(chan struct{}),
		outChan:          opts.outChan,
	}
	if tc.retryPolicy == nil {
		tc.retryPolicy = DefaultWatcherRetryPolicy
	}

	if tc.maxRelativeDepth > 0 && tc.minRelativeDepth > 0 && tc.maxRelativeDepth < tc.minRelativeDepth {
		return nil, fmt.Errorf("zk: Empty depth selection (min relative depth: %d, max relative depth: %d)",
			tc.minRelativeDepth, tc.maxRelativeDepth)
	}

	var watchMode AddWatchMode
	if opts.MinRelativeDepth == 1 && opts.MaxRelativeDepth == 1 {
		// special case: when both min and max are 1, then this cache will only watch the root node and ignore its
		// children, so a non-recursive watch is sufficient.
		watchMode = AddWatchModePersistent
	} else {
		watchMode = AddWatchModePersistentRecursive
	}

	tc.events, err = conn.AddPersistentWatch(rootPath, watchMode)
	if err != nil {
		return nil, err
	}
	tc.start()

	return tc, nil
}

func (tc *TreeCache[T]) start() {
	ch := toUnlimitedChannel(tc.events)

	tc.bootstrapRoot()

	go func() {
		for e := range ch {
			switch e.Type {
			case EventWatching:
				tc.bootstrapRoot()
			case EventNodeCreated:
				tc.nodeCreated(e.Path)
			case EventNodeDataChanged:
				tc.nodeDataChanged(e.Path)
			case EventNodeDeleted:
				tc.nodeDeleted(e.Path)
			case EventNotWatching:
				// EventNotWatching means that channel's about to close, and we can report the error that caused the
				// closure. We don't zero out the data to reflect the last known state of all the nodes.
				tc.visitAll(tc.RootPath, tc.root, false, func(_ string, n *treeCacheNode[T]) {
					n.err = e.Err
				})
			}
			if tc.outChan != nil {
				tc.outChan <- e
			}
		}
		if tc.outChan != nil {
			close(tc.outChan)
		}
	}()
}

func (tc *TreeCache[T]) visitAll(nodePath string, n *treeCacheNode[T], useReadLock bool, f func(nodePath string, n *treeCacheNode[T])) {
	var lock sync.Locker
	if useReadLock {
		lock = n.lock.RLocker()
	} else {
		lock = n.lock
	}
	lock.Lock()
	defer lock.Unlock()

	f(nodePath, n)
	for name, child := range n.children {
		tc.visitAll(JoinPath(nodePath, name), child, useReadLock, f)
	}
}

func (tc *TreeCache[T]) relativeDepth(nodePath string) int {
	if nodePath == tc.RootPath {
		return 1
	}
	nodePath = strings.TrimPrefix(nodePath, tc.RootPath)
	return strings.Count(nodePath, "/") + 1
}

func (tc *TreeCache[T]) checkAboveMinDepth(nodePath string) bool {
	return tc.minRelativeDepth > 0 && tc.relativeDepth(nodePath) < tc.minRelativeDepth
}

func (tc *TreeCache[T]) checkBelowMaxDepth(nodePath string) bool {
	return tc.maxRelativeDepth > 0 && tc.relativeDepth(nodePath) > tc.maxRelativeDepth
}

func (tc *TreeCache[T]) nodeCreated(nodePath string) {
	if tc.checkBelowMaxDepth(nodePath) {
		return
	}

	var n *treeCacheNode[T]
	if nodePath == tc.RootPath {
		n = tc.root
	} else {
		dir, name := SplitPath(nodePath)
		parent := tc.get(dir)
		if parent == nil {
			// This can happen if the node was created then immediately deleted while a bootstrap was occurring.
			return
		}

		child, ok := parent.children[name]
		if ok {
			n = child
		} else {
			n = newTreeCacheNode[T]()
			defer func() { // after the new node's data is updated, add it to its parent's children
				parent.lock.Lock()
				defer parent.lock.Unlock()
				parent.children[name] = n
			}()
		}
	}

	var data []byte
	var err error
	if !tc.checkAboveMinDepth(nodePath) {
		data, err = getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
		if err == ErrNoNode {
			tc.nodeDeleted(nodePath)
			return
		}
	}

	_, _, _ = tc.setNodeData(n, nodePath, data, err)
}

func (tc *TreeCache[T]) nodeDataChanged(nodePath string) {
	if tc.checkBelowMaxDepth(nodePath) {
		return
	}

	var n *treeCacheNode[T]
	if nodePath == tc.RootPath {
		n = tc.root
	} else {
		n = tc.get(nodePath)
		if n == nil {
			// This can happen if a number of now redundant events were queued up during a .bootstrap() call
			return
		}
	}

	var data []byte
	var err error
	if !tc.checkAboveMinDepth(nodePath) {
		data, err = getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
		if err == ErrNoNode {
			tc.nodeDeleted(nodePath)
			return
		}
	}

	_, _, _ = tc.setNodeData(n, nodePath, data, err)
}

func (tc *TreeCache[T]) nodeDeleted(nodePath string) {
	if tc.checkBelowMaxDepth(nodePath) {
		return
	}

	if nodePath == tc.RootPath {
		tc.root.lock.Lock()
		tc.root.data = nil
		tc.root.children = map[string]*treeCacheNode[T]{}
		tc.root.err = ErrNoNode
		tc.root.lock.Unlock()
	} else {
		dir, name := SplitPath(nodePath)
		parent := tc.get(dir)
		if parent == nil {
			// This can happen if a number of now redundant events were queued up during a .bootstrap() call
			return
		}
		parent.lock.Lock()
		parent.lock.Unlock()
		_, ok := parent.children[name]
		if ok {
			delete(parent.children, name)
		}
	}
}

var ErrNotInWatchedSubtree = fmt.Errorf("zk: node path is not in watched subtree")
var ErrAboveMinDepth = fmt.Errorf("zk: node path is above minimum configured depth")
var ErrBelowMaxDepth = fmt.Errorf("zk: node path is below maximum configured depth")

func (tc *TreeCache[T]) cleanAndCheckPath(nodePath string) (string, error) {
	if !strings.HasPrefix(nodePath, tc.RootPath) {
		return "", ErrNotInWatchedSubtree
	}
	if strings.HasSuffix(nodePath, "/") && nodePath != "/" {
		nodePath = nodePath[:len(nodePath)-1]
	}
	if tc.checkAboveMinDepth(nodePath) {
		return nodePath, ErrAboveMinDepth
	}
	if tc.checkBelowMaxDepth(nodePath) {
		return nodePath, ErrBelowMaxDepth
	}
	return nodePath, nil
}

// Get returns the node's most up-to-date data and children.
func (tc *TreeCache[T]) Get(nodePath string) (t T, data []byte, err error) {
	nodePath, err = tc.cleanAndCheckPath(nodePath)
	if err != nil {
		return t, nil, err
	}

	n := tc.get(nodePath)
	if n == nil {
		return t, nil, ErrNoNode
	}

	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.t, n.data, n.err
}

func (tc *TreeCache[T]) get(nodePath string) *treeCacheNode[T] {
	if nodePath == tc.RootPath {
		return tc.root
	}

	var relativeNodePath string
	if tc.RootPath == "/" {
		relativeNodePath = nodePath[1:]
	} else {
		relativeNodePath = nodePath[len(tc.RootPath)+1:]
	}
	segments := strings.Split(relativeNodePath, "/")

	node := tc.root
	for _, s := range segments {
		node.lock.RLock()
		newNode, ok := node.children[s]
		node.lock.RUnlock()
		if !ok {
			return nil
		}
		node = newNode
	}

	return node
}

// Refresh forces the immediate refresh of a node's data (not its children).
func (tc *TreeCache[T]) Refresh(nodePath string) (data []byte, t T, err error) {
	nodePath, err = tc.cleanAndCheckPath(nodePath)
	if err != nil {
		return nil, t, err
	}

	n := tc.get(nodePath)
	if n != nil {
		return nil, t, ErrNoNode
	}

	data, err = getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	if err == nil {
		var tmpT T
		tmpT, err = tc.unmarshaler(nodePath, data)
		if err == nil {
			t = tmpT
		}
	}

	return tc.setNodeData(n, nodePath, data, err)
}

func (tc *TreeCache[T]) setNodeData(n *treeCacheNode[T], nodePath string, data []byte, err error) ([]byte, T, error) {
	if err != nil {
		n.lock.Lock()
		defer n.lock.Unlock()
		n.err = err
		return n.data, n.t, n.err
	}

	t, err := tc.unmarshaler(nodePath, data)

	n.lock.Lock()
	defer n.lock.Unlock()

	n.data = data
	n.err = err
	if err == nil {
		n.t = t
	}
	return n.data, n.t, n.err
}

type NodeData[T any] struct {
	Err  error
	T    T
	Data []byte
}

// Children constructs a map of all the children of the given node.
func (tc *TreeCache[T]) Children(root string) (m map[string]NodeData[T]) {
	root, err := tc.cleanAndCheckPath(root)
	if err != nil && !(err == ErrAboveMinDepth && !tc.checkAboveMinDepth(root+"/")) {
		return map[string]NodeData[T]{root: {Err: err}}
	}
	n := tc.get(root)
	if n == nil {
		return map[string]NodeData[T]{root: {Err: ErrNoNode}}
	}

	m = map[string]NodeData[T]{}

	n.lock.RLock()
	defer n.lock.RUnlock()

	for k, v := range n.children {
		v.lock.RLock()
		m[k] = NodeData[T]{
			Err:  v.err,
			Data: v.data,
			T:    v.t,
		}
		v.lock.RUnlock()
	}

	return m
}

// Tree recursively constructs a map of all the nodes the cache is aware of, starting at the given root.
func (tc *TreeCache[T]) Tree(root string) (m map[string]NodeData[T]) {
	root, err := tc.cleanAndCheckPath(root)
	if err != nil && err != ErrAboveMinDepth { // ignore min depth errors as Tree recursively lists all nodes below root
		return map[string]NodeData[T]{root: {Err: err}}
	}
	n := tc.get(root)
	if n == nil {
		return map[string]NodeData[T]{root: {Err: ErrNoNode}}
	}

	m = map[string]NodeData[T]{}

	tc.visitAll(root, n, true, func(nodePath string, n *treeCacheNode[T]) {
		if !tc.checkAboveMinDepth(nodePath) {
			m[nodePath] = NodeData[T]{
				Err:  n.err,
				Data: n.data,
				T:    n.t,
			}
		}
	})

	return m
}

func newTreeCacheNode[T any]() *treeCacheNode[T] {
	return &treeCacheNode[T]{
		lock:     new(sync.RWMutex),
		children: map[string]*treeCacheNode[T]{},
	}
}

func (tc *TreeCache[T]) bootstrapRoot() {
	tc.bootstrap(tc.RootPath, tc.root, 1)
}

func (tc *TreeCache[T]) bootstrap(nodePath string, n *treeCacheNode[T], depth int) (deleted bool) {
	n.lock.Lock()
	defer n.lock.Unlock()

	// TODO: Use MultiRead to execute BFS scan of tree instead of DFS

	var data []byte
	var children []string
	var err error
	relativeDepth := tc.relativeDepth(nodePath)
	switch {
	case tc.maxRelativeDepth > 0 && relativeDepth >= tc.maxRelativeDepth:
		data, err = getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	case tc.minRelativeDepth > 0 && relativeDepth <= tc.minRelativeDepth:
		children, err = getNodeChildren(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	default:
		data, children, err = getNodeDataAndChildren(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	}
	if err != nil {
		n.err = err
		return err == ErrNoNode
	}

	childrenMap := make(map[string]*treeCacheNode[T], len(children))
	for _, c := range children {
		childrenMap[c] = newTreeCacheNode[T]()
	}

	for k, v := range childrenMap {
		if tc.bootstrap(JoinPath(nodePath, k), v, depth+1) {
			delete(childrenMap, k)
		}
	}

	t, err := tc.unmarshaler(nodePath, data)
	n.data, n.t, n.children, n.err = data, t, childrenMap, err

	return false
}

// Stop removes the persistent watch that was created for this path. Returns zk.ErrNoWatcher if called more than once.
func (tc *TreeCache[T]) Stop() (err error) {
	err = tc.Conn.RemovePersistentWatch(tc.RootPath, tc.events)
	if err == nil {
		close(tc.stopChan)
	}
	return err
}
