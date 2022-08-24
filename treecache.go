package zk

import (
	"fmt"
	"strings"
	"sync"
)

type TreeCacheOpts struct {
	// The retry policy to use when fetching nodes' data and children. If nil, uses DefaultWatcherRetryPolicy
	RetryPolicy RetryPolicy
	// Provides a depth limit when bootstrapping and listening to watch events. The root is considered depth 1, so any
	// negative or zero value disables this feature.
	MaxDepth int
	// used for debugging, receives events after they are processed
	outChan chan Event
}

type TreeCache struct {
	RootPath string
	Conn     *Conn

	root        *treeCacheNode
	retryPolicy RetryPolicy
	maxDepth    int

	events   <-chan Event
	stopChan chan struct{}
	outChan  chan Event
}

type treeCacheNode struct {
	lock     sync.RWMutex
	data     []byte
	children map[string]*treeCacheNode
	err      error
}

func NewTreeCache(conn *Conn, rootPath string) (tc *TreeCache, err error) {
	return NewTreeCacheWithOpts(conn, rootPath, TreeCacheOpts{})
}

func NewTreeCacheWithOpts(conn *Conn, rootPath string, opts TreeCacheOpts) (tc *TreeCache, err error) {
	tc = &TreeCache{
		RootPath:    rootPath,
		retryPolicy: opts.RetryPolicy,
		maxDepth:    opts.MaxDepth,
		root:        newTreeCacheNode(),
		Conn:        conn,
		stopChan:    make(chan struct{}),
		outChan:     opts.outChan,
	}
	if tc.retryPolicy == nil {
		tc.retryPolicy = DefaultWatcherRetryPolicy
	}

	tc.events, err = conn.AddPersistentWatch(rootPath, AddWatchModePersistentRecursive)
	if err != nil {
		return nil, err
	}
	tc.start()

	return tc, nil
}

func (tc *TreeCache) start() {
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
				// closure. We don't zero out the data and stat to reflect the last known state of all the nodes.
				var notWatching func(*treeCacheNode)
				notWatching = func(n *treeCacheNode) {
					n.lock.Lock()
					defer n.lock.Unlock()
					n.err = e.Err
					for _, c := range n.children {
						notWatching(c)
					}
				}
				notWatching(tc.root)
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

func (tc *TreeCache) isPastMaxDepth(nodePath string) bool {
	if tc.maxDepth <= 0 || nodePath == tc.RootPath {
		return false
	}

	nodePath = strings.TrimPrefix(nodePath, tc.RootPath+"/")
	return strings.Count(nodePath, "/")+1 >= tc.maxDepth
}

func (tc *TreeCache) nodeCreated(nodePath string) {
	if tc.isPastMaxDepth(nodePath) {
		return
	}

	var n *treeCacheNode
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
			n = newTreeCacheNode()
			defer func() { // after the new node's data is updated, add it to its parent's children
				parent.lock.Lock()
				defer parent.lock.Unlock()
				parent.children[name] = n
			}()
		}
	}

	data, err := getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	if err == ErrNoNode {
		tc.nodeDeleted(nodePath)
		return
	}

	n.lock.Lock()
	defer n.lock.Unlock()
	if err != nil {
		n.err = err
	} else {
		n.data, n.err = data, nil
	}
}

func (tc *TreeCache) nodeDataChanged(nodePath string) {
	if tc.isPastMaxDepth(nodePath) {
		return
	}

	var n *treeCacheNode
	if nodePath == tc.RootPath {
		n = tc.root
	} else {
		n = tc.get(nodePath)
		if n == nil {
			// This can happen if a number of now redundant events were queued up during a .bootstrap() call
			return
		}
	}

	data, err := getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	if err == ErrNoNode {
		tc.nodeDeleted(nodePath)
		return
	}

	n.lock.Lock()
	defer n.lock.Unlock()
	if err != nil {
		n.err = err
	} else {
		n.data, n.err = data, nil
	}
}

func (tc *TreeCache) nodeDeleted(nodePath string) {
	if tc.isPastMaxDepth(nodePath) {
		return
	}

	if nodePath == tc.RootPath {
		tc.root.lock.Lock()
		tc.root.data = nil
		tc.root.children = map[string]*treeCacheNode{}
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
var ErrPastMaxDepth = fmt.Errorf("zk: node path is past maximum configured depth")

func (tc *TreeCache) cleanAndCheckPath(nodePath string) (string, error) {
	if !strings.HasPrefix(nodePath, tc.RootPath) {
		return "", ErrNotInWatchedSubtree
	}
	if strings.HasSuffix(nodePath, "/") && nodePath != "/" {
		nodePath = nodePath[:len(nodePath)-1]
	}
	if tc.isPastMaxDepth(nodePath) {
		return "", ErrPastMaxDepth
	}
	return nodePath, nil
}

// Get returns the node's most up-to-date
func (tc *TreeCache) Get(nodePath string) (data []byte, children []string, err error) {
	nodePath, err = tc.cleanAndCheckPath(nodePath)
	if err != nil {
		return nil, nil, err
	}

	n := tc.get(nodePath)
	if n == nil {
		return nil, nil, ErrNoNode
	}

	n.lock.RLock()
	defer n.lock.RUnlock()
	data = n.data
	for k := range n.children {
		children = append(children, k)
	}
	return n.data, children, n.err
}

func (tc *TreeCache) get(nodePath string) *treeCacheNode {
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
func (tc *TreeCache) Refresh(nodePath string) (data []byte, err error) {
	nodePath, err = tc.cleanAndCheckPath(nodePath)
	if err != nil {
		return nil, err
	}

	n := tc.get(nodePath)
	if n != nil {
		return nil, ErrNoNode
	}

	data, err = getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)

	n.lock.Lock()
	defer n.lock.Unlock()
	n.data, n.err = data, err
	return n.data, n.err
}

type NodeData struct {
	Err  error
	Data []byte
}

// Children constructs a map of all the children of the given node.
func (tc *TreeCache) Children(root string) (m map[string]NodeData) {
	root, err := tc.cleanAndCheckPath(root)
	if err != nil {
		return map[string]NodeData{root: {Err: err}}
	}
	n := tc.get(root)
	if n == nil {
		return map[string]NodeData{root: {Err: ErrNoNode}}
	}

	m = map[string]NodeData{}

	n.lock.RLock()
	defer n.lock.RUnlock()

	for k, v := range n.children {
		v.lock.RLock()
		m[k] = NodeData{
			Err:  v.err,
			Data: v.data,
		}
		v.lock.RUnlock()
	}

	return m
}

// Tree recursively constructs a map of all the nodes the cache is aware of, starting at the given root.
func (tc *TreeCache) Tree(root string) (m map[string]NodeData) {
	root, err := tc.cleanAndCheckPath(root)
	if err != nil {
		return map[string]NodeData{root: {Err: err}}
	}
	n := tc.get(root)
	if n == nil {
		return map[string]NodeData{root: {Err: ErrNoNode}}
	}

	m = map[string]NodeData{}

	var tree func(string, *treeCacheNode)
	tree = func(nodePath string, n *treeCacheNode) {
		n.lock.RLock()
		defer n.lock.RUnlock()

		m[nodePath] = NodeData{
			Err:  n.err,
			Data: n.data,
		}

		for k, v := range n.children {
			tree(JoinPath(nodePath, k), v)
		}
	}

	tree(root, n)

	return m
}

func newTreeCacheNode() *treeCacheNode {
	return &treeCacheNode{children: map[string]*treeCacheNode{}}
}

func (tc *TreeCache) bootstrapRoot() {
	tc.bootstrap(tc.RootPath, tc.root, 1)
}

func (tc *TreeCache) bootstrap(nodePath string, n *treeCacheNode, depth int) (deleted bool) {
	n.lock.Lock()
	defer n.lock.Unlock()

	var data []byte
	var children []string
	var err error
	if tc.maxDepth > 0 && depth >= tc.maxDepth {
		data, err = getNodeData(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	} else {
		data, children, err = getNodeDataAndChildren(tc.retryPolicy, tc.stopChan, nodePath, tc.Conn)
	}
	if err != nil {
		n.err = err
		return n.err == ErrNoNode
	}

	var childrenMap map[string]*treeCacheNode

	childrenMap = make(map[string]*treeCacheNode, len(children))
	for _, c := range children {
		childrenMap[c] = newTreeCacheNode()
	}

	for k, v := range childrenMap {
		if tc.bootstrap(JoinPath(nodePath, k), v, depth+1) {
			delete(childrenMap, k)
		}
	}

	n.data, n.children, n.err = data, childrenMap, nil

	return false
}

// Stop removes the persistent watch that was created for this path. Returns zk.ErrNoWatcher if called more than once.
func (tc *TreeCache) Stop() (err error) {
	err = tc.Conn.RemovePersistentWatch(tc.RootPath, tc.events)
	if err == nil {
		close(tc.stopChan)
	}
	return err
}
