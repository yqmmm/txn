package txn

import (
	"strconv"
	"sync"
)

type record struct {
	mutex sync.RWMutex
	value string
}

type Naive struct {
	kv map[string]*record
}

func (n *Naive) Insert(k, v string) {
	r := n.kv[k]
	if r == nil {
		r = &record{
			mutex: sync.RWMutex{},
		}
	}
	r.value = v
	n.kv[k] = r
}

func (n *Naive) Txn() Txn {
	return &NaiveTxn{
		kv:     n.kv,
		rLocks: make(map[*sync.RWMutex]bool),
		wLocks: make(map[*sync.RWMutex]bool),
	}
}

// Naive just do 2PL, nothing else.
// It will deadlock.
type NaiveTxn struct {
	kv     map[string]*record
	rLocks map[*sync.RWMutex]bool
	wLocks map[*sync.RWMutex]bool
}

// Must be called or there will be lock unreleased
func (t *NaiveTxn) Commit() error {
	for rLock := range t.rLocks {
		rLock.RUnlock()
		delete(t.rLocks, rLock)
	}
	for wLock := range t.wLocks {
		wLock.Unlock()
		delete(t.wLocks, wLock)
	}
	return nil
}

func (t *NaiveTxn) Get(k string) (string, error) {
	r := t.kv[k]
	if r == nil {
		return "", NotFoundError{key: k}
	}

	mutex := &r.mutex
	if !t.rLocks[mutex] && !t.wLocks[mutex] {
		r.mutex.RLock()
		t.rLocks[mutex] = true
	}

	return r.value, nil
}

func (t *NaiveTxn) GetInt(k string) (int, error) {
	v, err := t.Get(k)
	if err != nil {
		return -1, err
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return -1, NotFoundError{key: k}
	}

	return i, nil
}

func (t *NaiveTxn) Update(k, v string) error {
	r := t.kv[k]
	if r == nil {
		return NotFoundError{key: k}
	}

	mutex := &r.mutex
	if !t.wLocks[mutex] {
		if t.rLocks[mutex] {
			mutex.RUnlock()
			delete(t.rLocks, mutex)
		}
		mutex.Lock()
		t.wLocks[mutex] = true
	}
	r.value = v
	return nil
}

func (t *NaiveTxn) UpdateInt(k string, v int) error {
	sv := strconv.Itoa(v)
	return t.Update(k, sv)
}
