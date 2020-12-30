package txn

import (
	"strconv"
	"sync/atomic"
)

type AbortError struct {
	by *LockTxn
}

func (e AbortError) Error() string {
	return "Aborted by" + strconv.Itoa(int(e.by.Timestamp))
}

//type LockClient struct {
//	Timestamp int
//}

type Lock interface {
	Lock(c *LockTxn) error
	Unlock(c *LockTxn) error
	RLock(c *LockTxn) error
	RUnlock(c *LockTxn) error
	Upgrade(c *LockTxn) error
}

type lockRecord struct {
	lock  Lock
	value string
}

type LockDB struct {
	kv              map[string]*lockRecord
	newLock         func() Lock
	globalTimestamp int32
}

func NewLockDB(newLock func() Lock) *LockDB {
	return &LockDB{
		kv:              make(map[string]*lockRecord),
		newLock:         newLock,
		globalTimestamp: 1,
	}
}

type LockTxn struct {
	kv        map[string]*lockRecord
	rLocks    map[Lock]bool
	wLocks    map[Lock]bool
	Timestamp int32
}

func (l *LockDB) Txn() Txn {
	swapped := false
	var old int32
	for !swapped {
		old = l.globalTimestamp
		swapped = atomic.CompareAndSwapInt32(&l.globalTimestamp, old, old+1)
	}
	return &LockTxn{
		kv:        l.kv,
		rLocks:    make(map[Lock]bool),
		wLocks:    make(map[Lock]bool),
		Timestamp: old + 1, // TODO
	}
}

func (l *LockDB) Insert(k, v string) {
	r := l.kv[k]
	if r == nil {
		r = &lockRecord{
			lock: l.newLock(), // TODO
		}
	}
	r.value = v
	l.kv[k] = r
}

func (t *LockTxn) Commit() error {
	for rLock := range t.rLocks {
		rLock.RUnlock(t)
		delete(t.rLocks, rLock)
	}
	for wLock := range t.wLocks {
		wLock.Unlock(t)
		delete(t.wLocks, wLock)
	}
	return nil
}

func (t *LockTxn) Get(k string) (string, error) {
	r := t.kv[k]
	if r == nil {
		return "", NotFoundError{key: k}
	}

	lock := r.lock
	if !t.rLocks[lock] && !t.wLocks[lock] {
		err := lock.RLock(t)
		if err != nil {
			t.Commit()
			return "", err
		}
	}

	return r.value, nil
}

func (t *LockTxn) GetInt(k string) (int, error) {
	v, err := t.Get(k)
	if err != nil {
		t.Commit()
		return -1, err
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return -1, NotFoundError{key: k}
	}

	return i, nil
}

func (t *LockTxn) Update(k, v string) error {
	r := t.kv[k]
	if r == nil {
		return NotFoundError{key: k}
	}

	mutex := r.lock
	if !t.wLocks[mutex] {
		var err error
		if t.rLocks[mutex] {
			err = mutex.Upgrade(t)
			if err != nil {
				t.Commit()
				return err
			}
			delete(t.rLocks, mutex)
		} else {
			err = mutex.Lock(t)
			if err != nil {
				t.Commit()
			}
		}
		t.wLocks[mutex] = true
	}

	r.value = v
	return nil
}

func (t *LockTxn) UpdateInt(k string, v int) error {
	sv := strconv.Itoa(v)
	return t.Update(k, sv)
}
