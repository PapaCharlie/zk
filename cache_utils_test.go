package zk

import (
	"fmt"
	"testing"
	"time"
)

func TestExecuteWithRetries(t *testing.T) {
	errors := make([]error, 5)
	for i := range errors {
		errors[i] = fmt.Errorf("%d", i)
	}
	idx := 0
	call := func() error {
		err := errors[idx]
		idx++
		return err
	}

	stopChan := make(chan struct{})

	err := ExecuteWithRetries(
		func(error) bool { return false },
		func(int) time.Duration { return 0 },
		stopChan,
		call,
	)
	if err != errors[0] {
		t.Fatalf("Did not get correct error: %+v", err)
	}

	idx = 0
	attempts := 0
	err = ExecuteWithRetries(
		func(error) bool {
			return idx <= 2
		},
		func(int) time.Duration {
			attempts++
			return 0
		},
		stopChan,
		call,
	)
	if err != errors[2] {
		t.Fatalf("Did not get correct error: %+v", err)
	}
	if attempts != 2 {
		t.Fatalf("Did not get correct attempt count: %d", attempts)
	}

	idx = 0
	attempts = 0
	err = ExecuteWithRetries(
		func(error) bool {
			return true
		},
		func(int) time.Duration {
			attempts++
			// If call is called again after stopChan is closed, it will panic because no more errors remain
			if attempts == 5 {
				close(stopChan)
			}
			return 0
		},
		stopChan,
		call,
	)
	if err != errors[4] {
		t.Fatalf("Did not get correct error: %+v", err)
	}
}
