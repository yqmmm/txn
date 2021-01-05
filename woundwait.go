package txn

import "sync"

type WoundWaitLock struct {
	writer  *LockTxn
	readers map[*LockTxn]bool

	mu            sync.Mutex
	broadcastChan chan struct{}
}

func NewWoundWaitLock() Lock {
	return &WoundWaitLock{
		writer:        nil,
		readers:       make(map[*LockTxn]bool),
		mu:            sync.Mutex{},
		broadcastChan: make(chan struct{}),
	}
}

// Should be called with lock acquired
func (l *WoundWaitLock) listen() <-chan struct{} {
	return l.broadcastChan
}

// Should be called with lock acquired
func (l *WoundWaitLock) broadcast() {
	newCh := make(chan struct{})

	ch := l.broadcastChan
	l.broadcastChan = newCh

	close(ch)
}

func (txn *LockTxn) CheckAbort() error {
	select {
	case by := <-txn.StopCh:
		return AbortError{
			by: by,
		}
	default:
		return nil
	}
}

func (l *WoundWaitLock) RLock(txn *LockTxn) error {
	//if err := txn.CheckAbort(); err != nil {
	//	return err
	//}

	for {
		l.mu.Lock()
		if l.writer != nil {
			if l.writer.Timestamp > txn.Timestamp {
				select {
				case l.writer.StopCh <- txn:
				default:
				}
			} // else: wait
		} else {
			l.readers[txn] = true
			l.mu.Unlock()
			return nil
		}

		broker := l.listen()
		l.mu.Unlock()
		select {
		case by := <-txn.StopCh:
			return AbortError{by: by}
		case <-broker:
		}
	}
}

func (l *WoundWaitLock) RUnlock(txn *LockTxn) error {
	l.mu.Lock()
	delete(l.readers, txn)
	l.broadcast()
	l.mu.Unlock()
	return nil
}

func (l *WoundWaitLock) Lock(txn *LockTxn) error {
	return l.lock(txn, false)
}

// For upgrade, if return error, the reader lock is not released
func (l *WoundWaitLock) lock(txn *LockTxn, upgrade bool) error {
	//if err := txn.CheckAbort(); err != nil {
	//	return err
	//}

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

		if l.writer != nil && l.writer.Timestamp > txn.Timestamp {
			select {
			case l.writer.StopCh <- txn:
			default:
			}
		} else {
			for reader := range l.readers {
				if reader.Timestamp > txn.Timestamp {
					select {
					case reader.StopCh <- txn:
					default:
					}
				}
			}
		}

		broker := l.listen()
		l.mu.Unlock()

		select {
		case by := <-txn.StopCh:
			return AbortError{by: by}
		case <-broker:
		}
	}
}

func (l *WoundWaitLock) Unlock(txn *LockTxn) error {
	l.mu.Lock()
	l.writer = nil
	l.broadcast()
	l.mu.Unlock()
	return nil
}

func (l *WoundWaitLock) Upgrade(txn *LockTxn) error {
	return l.lock(txn, true)
}
