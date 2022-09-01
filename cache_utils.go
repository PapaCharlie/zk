package zk

import (
	"math"
	"math/rand"
	"strings"
	"time"
)

type RetryPolicy interface {
	// ShouldRetry checks whether a given failed call should be retried based on how many times it was attempted and the
	// last error encountered. See ExecuteWithRetries for more details.
	ShouldRetry(attempt int, lastError error) (backoff time.Duration)
}

// ExecuteWithRetries simply retries the given function as many times as the given policy will allow, waiting in between
// invocations according to the backoff given by the policy. If the policy returns a negative backoff or stopChan is
// closed, the last encountered error is returned.
func ExecuteWithRetries(policy RetryPolicy, stopChan chan struct{}, f func() (err error)) (err error) {
	for attempt := 0; ; attempt++ {
		err = f()
		if err == nil {
			return nil
		}
		backoff := policy.ShouldRetry(attempt, err)
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

// The DefaultWatcherRetryPolicy is an ExponentialBackoffPolicy with infinite retries on all but three error types:
//
// - zk.ErrNoNode: Retrying fetches on a node that doesn't exist isn't going to yield very interesting results,
// especially in the context of a watch where an eventual zk.EventNodeCreated will notify the watcher of the node's
// reappearance.
//
// - zk.ErrConnectionClosed: This error is returned by any call made after Close() is called on a zk.Conn. This call
// will never succeed.
//
// - zk.ErrNoAuth: If a zk.Conn does not have the required authentication to access a node, retrying the call will not
// succeed until authentication is added. It's best to report this as early as possible instead of blocking the process.
//
// The reasoning behind infinite retries by default is that if any network connectivity issues arise, the watcher itself
// will likely be impacted or stop receiving events altogether. Retrying forever is the best bet to keep everything in
// sync.
var DefaultWatcherRetryPolicy RetryPolicy = &ExponentialBackoffPolicy{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     5 * time.Second,
	MaxAttempts:    math.MaxInt64,
	IsErrorRetryable: func(err error) bool {
		return err != ErrNoNode && err != ErrConnectionClosed && err != ErrNoAuth
	},
}

type RetryPolicyFunc func(attempt int, lastError error) time.Duration

func (r RetryPolicyFunc) ShouldRetry(attempt int, lastError error) (backoff time.Duration) {
	return r(attempt, lastError)
}

// ExponentialBackoffPolicy is a RetryPolicy that implements exponential backoff and jitter (see "Full Jitter" in
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
	// If non-nil, this function is called to check if an error can be retried
	IsErrorRetryable func(err error) bool
	// If non-nil, this rand.Rand will be used to generate the jitter. Otherwise, the global rand is used.
	Rand *rand.Rand
}

func (e *ExponentialBackoffPolicy) ShouldRetry(retryCount int, err error) (backoff time.Duration) {
	if (e.IsErrorRetryable != nil && !e.IsErrorRetryable(err)) || retryCount > e.MaxAttempts {
		return -1
	}

	backoff = e.InitialBackoff << retryCount
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

func getNodeData(policy RetryPolicy, stopChan chan struct{}, nodePath string, conn *Conn) (data []byte, err error) {
	err = ExecuteWithRetries(policy, stopChan, func() (err error) {
		data, _, err = conn.Get(nodePath)
		return err
	})
	return data, err
}

func getNodeChildren(policy RetryPolicy, stopChan chan struct{}, nodePath string, conn *Conn) (children []string, err error) {
	err = ExecuteWithRetries(policy, stopChan, func() (err error) {
		children, _, err = conn.Children(nodePath)
		return err
	})
	return children, err
}

func getNodeDataAndChildren(policy RetryPolicy, stopChan chan struct{}, nodePath string, conn *Conn) (data []byte, children []string, err error) {
	err = ExecuteWithRetries(policy, stopChan, func() (err error) {
		data, _, children, err = conn.GetDataAndChildren(nodePath)
		return err
	})
	return data, children, err
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
