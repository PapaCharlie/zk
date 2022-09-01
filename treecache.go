//go:build go1.18
// +build go1.18

package zk

import (
	"fmt"
	"strings"
	"sync"
)

type TreeCacheWatcher[T any] interface {
	OnCreateOrUpdate(path string, data NodeData[T])
	OnDelete(path string)
	OnBootstrapOrReconnect(tree map[string]NodeData[T])
}

type NodeFilter func(nodePath string) (getData, getChildren bool)

const DefaultBoostrapMultiReadLimit = 100

type TreeCacheOpts[T any] struct {
	// The backoff policy to use when fetching nodes' data and children. If nil, uses DefaultWatcherBackoffPolicy
	BackoffPolicy BackoffPolicy
	// If specified, the filter will be invoked every time the data or children for a specific node is fetched. Note
	// that the filter must be hierarchically consistent. In other words, getChildren must be true for /a and /a/b in
	// order for /a/b/c to be fetched correctly. Inconsistent filters will result in undefined behavior.
	Filter NodeFilter
	// If specified, the watcher's corresponding methods will be invoked whenever the state changes
	Watcher TreeCacheWatcher[T]
	// Defines the multi read limit for bootstrapping. If zero, DefaultBoostrapMultiReadLimit is used.
	BootstrapMultiReadLimit int
	// Used to receive a callback when events are processed, for debugging purposes. Ignored if nil.
	outChan chan Event
	// Indicates this tree cache is intended to back a NodeCache, and AddWatchModePersistent should be used instead of
	// AddWatchModePersistentRecursive
	isNodeCache bool
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

	root          *treeCacheNode[T]
	rootLock      sync.RWMutex
	unmarshaler   func(path string, data []byte) (T, error)
	backoffPolicy BackoffPolicy

	events                  <-chan Event
	stopChan                chan struct{}
	outChan                 chan Event
	watcher                 TreeCacheWatcher[T]
	filter                  NodeFilter
	bootstrapMultiReadLimit int
}

type treeCacheNode[T any] struct {
	lock *sync.RWMutex
	NodeData[T]
	children map[string]*treeCacheNode[T]
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
	return NewTreeCacheWithOpts[T](conn, rootPath, unmarshaler, TreeCacheOpts[T]{})
}

func NewTreeCacheWithOpts[T any](
	conn *Conn,
	rootPath string,
	unmarshaler func(path string, data []byte) (T, error),
	opts TreeCacheOpts[T],
) (tc *TreeCache[T], err error) {
	tc = &TreeCache[T]{
		RootPath:                rootPath,
		unmarshaler:             unmarshaler,
		backoffPolicy:           opts.BackoffPolicy,
		Conn:                    conn,
		stopChan:                make(chan struct{}),
		outChan:                 opts.outChan,
		watcher:                 opts.Watcher,
		filter:                  opts.Filter,
		bootstrapMultiReadLimit: opts.BootstrapMultiReadLimit,
	}
	tc.setRoot(newTreeCacheNode[T]())
	if tc.backoffPolicy == nil {
		tc.backoffPolicy = DefaultWatcherBackoffPolicy
	}
	if tc.filter == nil {
		tc.filter = func(string) (getData, getChildren bool) { return true, true }
	}
	if tc.bootstrapMultiReadLimit == 0 {
		tc.bootstrapMultiReadLimit = DefaultBoostrapMultiReadLimit
	}

	var watchMode AddWatchMode
	if opts.isNodeCache {
		watchMode = AddWatchModePersistent
		tc.filter = func(nodePath string) (getData, getChildren bool) {
			return true, false
		}
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

func (tc *TreeCache[T]) setRoot(root *treeCacheNode[T]) {
	tc.rootLock.Lock()
	defer tc.rootLock.Unlock()
	tc.root = root
}

func (tc *TreeCache[T]) getRoot() *treeCacheNode[T] {
	tc.rootLock.RLock()
	defer tc.rootLock.RUnlock()
	return tc.root
}

func (tc *TreeCache[T]) start() {
	ch := ToUnlimitedChannel(tc.events)

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
				tc.visitAll(tc.RootPath, tc.getRoot(), false, func(_ string, n *treeCacheNode[T]) {
					n.Err = e.Err
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

func (tc *TreeCache[T]) nodeCreated(nodePath string) {
	getData, getChildren := tc.filter(nodePath)
	if !(getData || getChildren) {
		return
	}

	var n *treeCacheNode[T]
	var parent *treeCacheNode[T]
	if nodePath == tc.RootPath {
		n = tc.getRoot()
	} else {
		dir, name := SplitPath(nodePath)
		parent = tc.get(dir)
		if parent == nil {
			// This can happen if the node was created then immediately deleted along with its parent while a bootstrap
			// was occurring and the delete event has not yet been processed.
			return
		}

		child, ok := parent.children[name]
		if ok {
			n = child
		} else {
			n = newTreeCacheNode[T]()
			defer func() { // after the new node's data is updated, add it to its parent's children
				if n.Err != ErrNoNode {
					parent.lock.Lock()
					defer parent.lock.Unlock()
					parent.children[name] = n
				}
			}()
		}
	}

	if getData {
		data, stat, err := GetWithRetries(tc.backoffPolicy, tc.stopChan, tc.Conn, nodePath)
		if err == ErrNoNode {
			tc.nodeDeleted(nodePath)
			return
		}
		tc.setNodeData(n, nodePath, data, stat, err)
	}
}

func (tc *TreeCache[T]) nodeDataChanged(nodePath string) {
	getData, _ := tc.filter(nodePath)
	if !getData {
		return
	}

	var n *treeCacheNode[T]
	if nodePath == tc.RootPath {
		n = tc.getRoot()
	} else {
		n = tc.get(nodePath)
		if n == nil {
			// This can happen if a number of now redundant events were queued up during a .bootstrap() call
			return
		}
	}

	data, stat, err := GetWithRetries(tc.backoffPolicy, tc.stopChan, tc.Conn, nodePath)
	if err == ErrNoNode {
		tc.nodeDeleted(nodePath)
		return
	}
	tc.setNodeData(n, nodePath, data, stat, err)
}

func (tc *TreeCache[T]) nodeDeleted(nodePath string) {
	getData, _ := tc.filter(nodePath)
	if tc.watcher != nil && getData {
		defer tc.watcher.OnDelete(nodePath)
	}

	if nodePath == tc.RootPath {
		root := newTreeCacheNode[T]()
		root.Err = ErrNoNode
		tc.setRoot(root)
	} else {
		dir, name := SplitPath(nodePath)
		parent := tc.get(dir)
		if parent == nil {
			// This can happen if a number of now redundant events were queued up during a .bootstrap() call
			return
		}
		parent.lock.Lock()
		defer parent.lock.Unlock()
		delete(parent.children, name)
	}
}

var (
	ErrNotInWatchedSubtree = fmt.Errorf("zk: node path is not in watched subtree")
	ErrNodeIgnored         = fmt.Errorf("zk: node is ignored")
)

func (tc *TreeCache[T]) cleanAndCheckPath(nodePath string) (string, error) {
	if !strings.HasPrefix(nodePath, tc.RootPath) {
		return "", ErrNotInWatchedSubtree
	}
	if strings.HasSuffix(nodePath, "/") && nodePath != "/" {
		nodePath = nodePath[:len(nodePath)-1]
	}
	if getData, getChildren := tc.filter(nodePath); !(getData || getChildren) {
		return nodePath, ErrNodeIgnored
	}
	return nodePath, nil
}

// Get returns the node's most up-to-date data and children.
func (tc *TreeCache[T]) Get(nodePath string) (t T, data []byte, stat Stat, err error) {
	nodePath, err = tc.cleanAndCheckPath(nodePath)
	if err != nil {
		return t, nil, stat, err
	}

	n := tc.get(nodePath)
	if n == nil {
		return t, nil, stat, ErrNoNode
	}

	n.lock.RLock()
	defer n.lock.RUnlock()
	return n.T, n.Data, n.Stat, n.Err
}

func (tc *TreeCache[T]) get(nodePath string) *treeCacheNode[T] {
	if nodePath == tc.RootPath {
		return tc.getRoot()
	}

	var relativeNodePath string
	if tc.RootPath == "/" {
		relativeNodePath = nodePath[1:]
	} else {
		relativeNodePath = nodePath[len(tc.RootPath)+1:]
	}
	segments := strings.Split(relativeNodePath, "/")

	node := tc.getRoot()
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

func (tc *TreeCache[T]) setNodeData(n *treeCacheNode[T], nodePath string, data []byte, stat *Stat, err error) NodeData[T] {
	if err != nil {
		n.lock.Lock()
		defer n.lock.Unlock()
		n.Err = err
		return n.NodeData
	}

	t, err := tc.unmarshaler(nodePath, data)

	n.lock.Lock()
	defer func() {
		// grab a copy of the data, then release the lock, then invoke the watcher. This way the watcher's execution
		// does not block subsequent reads on this node
		nd := n.NodeData
		n.lock.Unlock()
		if nd.Err == nil && tc.watcher != nil {
			tc.watcher.OnCreateOrUpdate(nodePath, nd)
		}
	}()

	n.Data = data
	n.Stat = *stat
	n.Err = err
	if err == nil {
		n.T = t
	}
	return n.NodeData
}

type NodeData[T any] struct {
	Err  error
	T    T
	Data []byte
	Stat Stat
}

// Children constructs a map of all the children of the given node.
func (tc *TreeCache[T]) Children(root string) (m map[string]NodeData[T]) {
	root, err := tc.cleanAndCheckPath(root)
	if err != nil {
		return map[string]NodeData[T]{root: {Err: err}}
	}

	if _, getChildren := tc.filter(root); !getChildren {
		return nil
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
		m[k] = v.NodeData
		v.lock.RUnlock()
	}

	return m
}

// RootTree is the equivalent of calling Tree with TreeCache.RootPath as the parameter
func (tc *TreeCache[T]) RootTree() (m map[string]NodeData[T]) {
	return tc.Tree(tc.RootPath)
}

// Tree recursively constructs a map of all the nodes the cache is aware of, starting at the given root.
func (tc *TreeCache[T]) Tree(root string) (m map[string]NodeData[T]) {
	root, err := tc.cleanAndCheckPath(root)
	if err != nil { // ignore min depth errors as Tree recursively lists all nodes below root
		return map[string]NodeData[T]{root: {Err: err}}
	}
	if _, getChildren := tc.filter(root); !getChildren {
		return nil
	}
	n := tc.get(root)
	if n == nil {
		return map[string]NodeData[T]{root: {Err: ErrNoNode}}
	}

	m = map[string]NodeData[T]{}

	tc.visitAll(root, n, true, func(nodePath string, n *treeCacheNode[T]) {
		if getData, _ := tc.filter(nodePath); getData {
			m[nodePath] = n.NodeData
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
	tc.setRoot(tc.bootstrap())
	if tc.watcher != nil {
		tc.watcher.OnBootstrapOrReconnect(tc.RootTree())
	}
}

func (tc *TreeCache[T]) bootstrap() (root *treeCacheNode[T]) {
	root = newTreeCacheNode[T]()

	currentTier := map[string]*treeCacheNode[T]{
		tc.RootPath: root,
	}

	var ops []ReadOp
	for len(currentTier) > 0 {
		for nodePath, n := range currentTier {
			getData, getChildren := tc.filter(nodePath)
			if getData {
				ops = append(ops, GetDataOp(nodePath))
			}
			if getChildren {
				ops = append(ops, GetChildrenOp(nodePath))
			}
			if !(getData || getChildren) {
				n.Err = ErrNodeIgnored
			}
		}

		res, err := MultiReadWithRetries(tc.backoffPolicy, tc.stopChan, tc.Conn, tc.bootstrapMultiReadLimit, ops...)
		if err != nil {
			for _, n := range currentTier {
				n.Err = err
			}
			return
		}

		nextTier := map[string]*treeCacheNode[T]{}
		for i, r := range res {
			op := ops[i]
			n := currentTier[op.GetPath()]

			if r.Err != nil {
				n.Err = r.Err
				continue
			}

			if op.IsGetData() {
				n.Data = r.Data
				n.Stat = *r.Stat
				n.T, n.Err = tc.unmarshaler(op.GetPath(), n.Data)
			} else {
				for _, c := range r.Children {
					child := newTreeCacheNode[T]()
					n.children[c] = child
					nextTier[JoinPath(op.GetPath(), c)] = child
				}
			}
		}

		currentTier = nextTier
		ops = ops[:0]
	}

	var clean func(n *treeCacheNode[T])
	clean = func(n *treeCacheNode[T]) {
		for name, child := range n.children {
			if child.Err == ErrNoNode || child.Err == ErrNodeIgnored {
				delete(n.children, name)
			} else {
				clean(child)
			}
		}
	}
	clean(root)

	return root
}

// Stop removes the persistent watch that was created for this path. Returns zk.ErrNoWatcher if called more than once.
func (tc *TreeCache[T]) Stop() (err error) {
	err = tc.Conn.RemovePersistentWatch(tc.RootPath, tc.events)
	if err == nil {
		close(tc.stopChan)
	}
	return err
}
