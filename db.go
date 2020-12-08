package txn

import (
	"errors"
	"strconv"
	"sync"
)

type Record struct {
	mutex sync.RWMutex
	value string
}

type DB struct {
	store map[string]*Record
}

type Txn struct {
	db     *DB
	rLocks map[*sync.RWMutex]bool
	wLocks map[*sync.RWMutex]bool
}

func NewDb() *DB {
	return &DB{
		store: make(map[string]*Record),
	}
}

func (db *DB) Txn() Txn {
	return Txn{
		db:     db,
		rLocks: make(map[*sync.RWMutex]bool),
		wLocks: make(map[*sync.RWMutex]bool),
	}
}

func (txn *Txn) Begin() {
}

func (txn *Txn) Commit() {
	for rLock := range txn.rLocks {
		rLock.RUnlock()
	}
	for wLock := range txn.wLocks {
		wLock.Unlock()
	}
}

func (txn *Txn) Get(k string) (string, error) {
	r := txn.db.store[k]
	if r == nil {
		return "", errors.New("NotFound")
	}

	if !txn.rLocks[&r.mutex] && !txn.wLocks[&r.mutex] {
		r.mutex.RLock()
		txn.rLocks[&r.mutex] = true
	}

	return r.value, nil
}

func (txn *Txn) GetInt(k string) (int, error) {
	v, err := txn.Get(k)
	if err != nil {
		return -1, err
	}

	i, err := strconv.Atoi(v)
	if err != nil {
		return -1, errors.New("NotFound")
	}

	return i, nil
}

func (txn *Txn) Update(k, v string) error {
	r := txn.db.store[k]
	if r == nil {
		return errors.New("NotFound")
	}

	mutex := &r.mutex
	if !txn.wLocks[mutex] {
		if txn.rLocks[mutex] {
			mutex.RUnlock()
			delete(txn.rLocks, mutex)
		}
		mutex.Lock()
		txn.wLocks[mutex] = true
	}
	r.value = v
	return nil
}

func (txn *Txn) UpdateInt(k string, v int) error {
	sv := strconv.Itoa(v)
	return txn.Update(k, sv)
}

// map is not thread safe, so do this before test
func (db *DB) Insert(k, v string) {
	r := db.store[k]
	if r == nil {
		r = &Record{
			mutex: sync.RWMutex{},
		}
	}

	r.value = v
	db.store[k] = r
}
