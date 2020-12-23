package txn

import (
	"sync"
)

type WaitDieLock struct {
	mu      sync.Mutex
	rw      sync.RWMutex
	Readers map[*LockTxn]bool
	writer  *LockTxn
}

func NewWaitDieLock() Lock {
	return &WaitDieLock{
		mu:      sync.Mutex{},
		rw:      sync.RWMutex{},
		Readers: make(map[*LockTxn]bool),
		writer:  nil,
	}
}

func (l *WaitDieLock) RLock(c *LockTxn) error {
	l.mu.Lock()

	if l.writer != nil && l.writer.Timestamp < c.Timestamp {
		return AbortError{by: l.writer}
	}

	l.Readers[c] = true
	l.mu.Unlock()
	// TODO: this two is not atomic
	l.rw.RLock()

	return nil
}

func (l *WaitDieLock) RUnlock(c *LockTxn) error {
	l.mu.Lock()
	delete(l.Readers, c)
	l.mu.Unlock()

	l.rw.RUnlock()
	return nil
}

func (l *WaitDieLock) Lock(c *LockTxn) error {
	l.mu.Lock()
	if l.writer != nil && l.writer.Timestamp < c.Timestamp {
		return AbortError{by: l.writer}
	}

	for client := range l.Readers {
		if client.Timestamp < c.Timestamp {
			return AbortError{by: l.writer}
		}
	}
	l.mu.Unlock()
	// TODO: this two is not atomic
	l.rw.Lock()
	l.writer = c // We can do this because we have the write lock?

	return nil
}

func (l *WaitDieLock) Unlock(c *LockTxn) error {
	l.writer = nil // We can do this because we have the write lock?
	l.rw.Unlock()
	return nil
}
