package zk

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrorFilter determines whether the given error can be retried, or if the call should be abandoned.
type ErrorFilter func(err error) (canRetry bool)

// BackoffPolicy computes how long ExecuteWithRetries should wait between failed attempts. If this returns a negative
// value, ExecuteWithRetries will exit with the last encountered error.
type BackoffPolicy func(attempt int) (backoff time.Duration)

// ExecuteWithRetries simply retries the given call as many times as the given ErrorFilter will allow, waiting in
// between attempts according to the BackoffPolicy. If the error filter says an error cannot be retried, or the policy
// returns a negative backoff or stopChan is closed, the last encountered error is returned.
func ExecuteWithRetries(
	filter ErrorFilter,
	policy BackoffPolicy,
	stopChan <-chan struct{},
	call func() (err error),
) (err error) {
	for attempt := 0; ; attempt++ {
		err = call()
		if err == nil {
			return nil
		}

		if !filter(err) {
			return err
		}

		backoff := policy(attempt)
		if backoff < 0 {
			return err
		}

		select {
		case <-stopChan:
			return err
		case <-time.After(backoff):
			continue
		}
	}
}

// The DefaultWatcherBackoffPolicy is an ExponentialBackoffPolicy with infinite retries. The reasoning behind infinite
// retries by default is that if any network connectivity issues arise, the watcher itself will likely be impacted or
// stop receiving events altogether. Retrying forever is the best bet to keep everything in sync.
var DefaultWatcherBackoffPolicy BackoffPolicy = (&ExponentialBackoffPolicy{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     5 * time.Second,
	MaxAttempts:    math.MaxInt64,
}).ComputeBackoff

type RetryPolicyFunc func(attempt int, lastError error) time.Duration

func (r RetryPolicyFunc) ComputeBackoff(attempt int, lastError error) (backoff time.Duration) {
	return r(attempt, lastError)
}

// ExponentialBackoffPolicy is a BackoffPolicy that implements exponential backoff and jitter (see "Full Jitter" in
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/). It gives an option to dynamically decide
// whether to retry specific error types.
type ExponentialBackoffPolicy struct {
	// The initial backoff duration and the value that will be multiplied when calculating the backoff for a specific
	// attempt.
	InitialBackoff time.Duration
	// The maximum duration to backoff.
	MaxBackoff time.Duration
	// How many times to retry a given call before bailing.
	MaxAttempts int
	// If non-nil, this rand.Rand will be used to generate the jitter. Otherwise, the global rand is used.
	Rand *rand.Rand
}

func (e *ExponentialBackoffPolicy) ComputeBackoff(attempt int) (backoff time.Duration) {
	if attempt > e.MaxAttempts {
		return -1
	}

	backoff = e.InitialBackoff << attempt
	if backoff < e.InitialBackoff /* check for overflow from left shift */ || backoff > e.MaxBackoff {
		backoff = e.MaxBackoff
	}

	if e.Rand != nil {
		backoff = time.Duration(e.Rand.Int63n(int64(backoff)))
	} else {
		backoff = time.Duration(rand.Int63n(int64(backoff)))

	}

	return backoff
}

// This default error filter will attempt to retry all but the following three errors:
// - zk.ErrNoNode: Retrying fetches on a node that doesn't exist isn't going to yield very interesting results,
// especially in the context of a watch where an eventual zk.EventNodeCreated will notify a watcher of the node's
// reappearance.
//
// - zk.ErrConnectionClosed: This error is returned by any call made after Close() is called on a zk.Conn. This call
// will never succeed.
//
// - zk.ErrNoAuth: If a zk.Conn does not have the required authentication to access a node, retrying the call will not
// succeed until authentication is added. It's best to report this as early as possible instead of blocking the process.
var DefaultErrorFilter = func(err error) bool {
	return err != ErrNoNode && err != ErrConnectionClosed && err != ErrNoAuth
}

// GetWithRetries attempts to fetch the given node's data using ExecuteWithRetries and the DefaultErrorFilter.
func GetWithRetries(
	conn *Conn,
	nodePath string,
	policy BackoffPolicy,
	stopChan <-chan struct{},
) (data []byte, stat *Stat, err error) {
	err = ExecuteWithRetries(
		DefaultErrorFilter,
		policy,
		stopChan,
		func() (err error) {
			data, stat, err = conn.Get(nodePath)
			return err
		},
	)
	return data, stat, err
}

// MultiReadWithRetries batches the given ops by the given batchLimit and executes the batches in parallel, up to
// maxConcurrentBatches. Much like how Conn.MultiRead is expected to behave, each MultiReadResponse will correspond to
// the given ReadOp in the same position.
func MultiReadWithRetries(
	conn *Conn,
	policy BackoffPolicy,
	stopChan <-chan struct{},
	batchLimit int,
	maxConcurrentBatches int,
	ops ...ReadOp,
) (responses []MultiReadResponse, err error) {
	if policy == nil {
		policy = DefaultWatcherBackoffPolicy
	}
	if batchLimit <= 0 {
		batchLimit = DefaultBoostrapMultiReadLimit
	}
	if maxConcurrentBatches <= 0 {
		maxConcurrentBatches = DefaultBootstrapMaxConcurrentMultiReadBatches
	}

	var firstErr atomic.Pointer[error]

	responses = make([]MultiReadResponse, len(ops))

	type bounds struct {
		start, end int
	}

	work := make(chan bounds, maxConcurrentBatches)

	wg := sync.WaitGroup{}
	wg.Add(maxConcurrentBatches)
	for i := 0; i < maxConcurrentBatches; i++ {
		go func() {
			defer wg.Done()
			for b := range work {
				var batchRes []MultiReadResponse
				err := ExecuteWithRetries(
					func(err error) (canRetry bool) {
						return err != ErrConnectionClosed
					},
					policy,
					stopChan,
					func() (err error) {
						batchRes, err = conn.MultiRead(ops[b.start:b.end]...)
						return err
					},
				)
				if err != nil {
					firstErr.CompareAndSwap(nil, &err)
					return
				}
				copy(responses[b.start:b.end], batchRes)
			}
		}()
	}

	for offset := 0; offset < len(ops); offset += batchLimit {
		b := bounds{
			start: offset,
			end:   offset + batchLimit,
		}
		if b.end > len(ops) {
			b.end = len(ops)
		}
		work <- b
	}
	close(work)
	wg.Wait()

	if errPtr := firstErr.Load(); errPtr != nil {
		err = *errPtr
	}
	return responses, err
}

func JoinPath(parent, child string) string {
	if !strings.HasSuffix(parent, "/") {
		parent += "/"
	}
	if strings.HasPrefix(child, "/") {
		child = child[1:]
	}
	return parent + child
}

func SplitPath(path string) (dir, name string) {
	i := strings.LastIndex(path, "/")
	if i == 0 {
		dir, name = "/", path[1:]
	} else {
		dir, name = path[:i], path[i+1:]
	}
	return dir, name
}

type NodeData struct {
	Err  error
	Data []byte
	Stat Stat
}

type Node struct {
	NodeData
	Children map[string]*Node
}

func (n *Node) ToMap(rootPath string) map[string]NodeData {
	if n == nil {
		return map[string]NodeData{rootPath: {Err: ErrNoNode}}
	}

	m := map[string]NodeData{}
	var toMap func(nodePath string, n *Node)
	toMap = func(nodePath string, n *Node) {
		m[nodePath] = n.NodeData
		for k, v := range n.Children {
			toMap(JoinPath(nodePath, k), v)
		}
	}
	toMap(rootPath, n)
	return m
}

const DefaultBoostrapMultiReadLimit = 100
const DefaultBootstrapMaxConcurrentMultiReadBatches = 10

type NodeFilter func(nodePath string) (getData, getChildren bool)

var DefaultNodeFilter = NodeFilter(func(nodePath string) (getData, getChildren bool) {
	return true, true
})

var ErrNodeIgnored = fmt.Errorf("zk: node is ignored")

func Bootstrap(
	conn *Conn,
	rootPath string,
	filter NodeFilter,
	backoffPolicy BackoffPolicy,
	stopChan <-chan struct{},
	multiReadLimit int,
	maxConcurrentMultiReadBatches int,
) (root *Node) {
	root = &Node{Children: map[string]*Node{}}
	if filter == nil {
		filter = DefaultNodeFilter
	}
	if backoffPolicy == nil {
		backoffPolicy = DefaultWatcherBackoffPolicy
	}
	if stopChan == nil {
		stopChan = make(chan struct{})
	}
	if multiReadLimit <= 0 {
		multiReadLimit = DefaultBoostrapMultiReadLimit
	}
	if maxConcurrentMultiReadBatches <= 0 {
		multiReadLimit = DefaultBootstrapMaxConcurrentMultiReadBatches
	}

	currentTier := map[string]*Node{
		rootPath: root,
	}

	var ops []ReadOp
	for len(currentTier) > 0 {
		for nodePath, n := range currentTier {
			getData, getChildren := filter(nodePath)
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

		res, err := MultiReadWithRetries(
			conn,
			backoffPolicy,
			stopChan,
			multiReadLimit,
			maxConcurrentMultiReadBatches,
			ops...,
		)
		if err != nil {
			for _, n := range currentTier {
				n.Err = err
			}
			return root
		}

		nextTier := map[string]*Node{}
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
			} else {
				for _, c := range r.Children {
					child := &Node{Children: map[string]*Node{}}
					n.Children[c] = child
					nextTier[JoinPath(op.GetPath(), c)] = child
				}
			}
		}

		currentTier = nextTier
		ops = ops[:0]
	}

	var clean func(n *Node)
	clean = func(n *Node) {
		for name, child := range n.Children {
			if child.Err == ErrNoNode || child.Err == ErrNodeIgnored {
				delete(n.Children, name)
			} else {
				clean(child)
			}
		}
	}
	clean(root)

	return root
}
