package breaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Opened
	HalfOpened
)

var (
	ErrOpened = errors.New("Opened")
)

type CircuitBreaker struct {
	mu               sync.Mutex
	failures         int
	threshold        int
	state            State
	resetTimeout     time.Duration
	lastStateChanged time.Time
}

func New(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:        threshold,
		resetTimeout:     timeout,
		lastStateChanged: time.Now().UTC(),
	}
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()

	if cb.state == Opened {
		if time.Since(cb.lastStateChanged) < cb.resetTimeout {
			return ErrOpened
		}

		cb.state = HalfOpened
	}

	if cb.state == HalfOpened {
		if err := fn(); err != nil {
			cb.failures++
			cb.state = Opened
			cb.lastStateChanged = time.Now().UTC()
			return err
		}
		cb.state = Closed
		cb.failures = 0
		return nil
	}

	if err := fn(); err != nil {
		cb.failures++
		if cb.failures >= cb.threshold {
			cb.state = Opened
			cb.lastStateChanged = time.Now().UTC()
		}
		return err
	}

	cb.failures = 0
	cb.state = Closed
	return nil
}
