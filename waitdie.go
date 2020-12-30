package txn

import "sync"

type WaitDieLock struct {
	writer  *LockTxn
	readers map[*LockTxn]bool

	mu            sync.Mutex
	broadcastChan chan struct{}
}

func NewWaitDieLock() Lock {
	return &WaitDieLock{
		writer:        nil,
		readers:       make(map[*LockTxn]bool),
		mu:            sync.Mutex{},
		broadcastChan: make(chan struct{}),
	}
}

// Should be called with lock acquired
func (l *WaitDieLock) listen() <-chan struct{} {
	return l.broadcastChan
}

// Should be called with lock acquired
func (l *WaitDieLock) broadcast() {
	newCh := make(chan struct{})

	ch := l.broadcastChan
	l.broadcastChan = newCh

	close(ch)
}

func (l *WaitDieLock) RLock(txn *LockTxn) error {
	for {
		l.mu.Lock()
		if l.writer != nil {
			if l.writer.Timestamp < txn.Timestamp {
				l.mu.Unlock()
				return AbortError{by: l.writer}
			} // else: wait
		} else {
			l.mu.Unlock()
			return nil
		}

		broker := l.listen()
		l.mu.Unlock()
		select {
		case <-broker:
		}
	}
}

func (l *WaitDieLock) RUnlock(txn *LockTxn) error {
	l.mu.Lock()
	delete(l.readers, txn)
	l.broadcast()
	l.mu.Unlock()
	return nil
}

func (l *WaitDieLock) Lock(txn *LockTxn) error {
	return l.lock(txn, false)
}

// For upgrade, if return error, the reader lock is not released
func (l *WaitDieLock) lock(txn *LockTxn, upgrade bool) error {
	for {
		l.mu.Lock()

		if l.writer == nil && ((!upgrade && len(l.readers) == 0) || (upgrade && len(l.readers) == 1)) {
			if upgrade {
				delete(l.readers, txn)
			}
			l.writer = txn
			l.mu.Unlock()
			return nil
		}

		var abortBy *LockTxn
		if l.writer != nil && l.writer.Timestamp < txn.Timestamp {
			abortBy = l.writer
		} else {
			for reader := range l.readers {
				if reader.Timestamp < txn.Timestamp {
					abortBy = reader
					break
				}
			}
		}
		if abortBy != nil {
			l.mu.Unlock()
			return AbortError{by: abortBy}
		}

		broker := l.listen()
		l.mu.Unlock()

		select {
		case <-broker:
		}
	}
}

func (l *WaitDieLock) Unlock(txn *LockTxn) error {
	l.mu.Lock()
	l.writer = nil
	l.broadcast()
	l.mu.Unlock()
	return nil
}

func (l *WaitDieLock) Upgrade(txn *LockTxn) error {
	return l.lock(txn, true)
}
