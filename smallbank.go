package txn

import (
	"math/rand"
)

// Table: account, saving, checking
// https://www.comp.nus.edu.sg/~cs5226/papers/si-cost-icde08.pdf

// TODO: investigate better string concat methods
type SmallBank struct {
	db        *DB
	customers []string
}

type SmallBankConfig struct {
	Customers        int
	HotspotCustomers int  // 90% operation operates on hotspot customers
	UniformOperation bool // false means 60% Bal operation
}

func NewSmallBank(config *SmallBankConfig) *SmallBank {

	s := &SmallBank{
		db:        NewDb(),
		customers: make([]string, config.Customers),
	}

	customers := make(map[string]bool)
	ids := make(map[string]bool)
	for i := 0; i < config.Customers; i++ {
		name := randSeq(8)
		for customers[name] {
			name = randSeq(8)
		}
		customers[name] = true

		id := randSeq(8)
		for ids[id] {
			id = randSeq(8)
		}

		// TODO: What number to use? seems like a math problem
		s.db.Insert("account:"+name, id)
		s.db.Insert("saving:"+id, "1000")
		s.db.Insert("checking:"+id, "1000")
	}

	for c := range customers {
		s.customers = append(s.customers, c)
	}

	return s
}

func (s *SmallBank) Test() {
	name := s.customers[rand.Intn(len(s.customers))]

	switch idx := rand.Intn(5); idx {
	case 0:
		s.db.Bal(name)
	case 1:
		s.db.DepositChecking(name, rand.Intn(100))
	case 2:
		s.db.TransactSaving(name, rand.Intn(100))
	case 3:
		anotherName := s.customers[rand.Intn(len(s.customers))]
		s.db.Amalgamate(name, anotherName)
	case 4:
		s.db.WriteCheck(name, rand.Intn(100))
	}
}

func (db *DB) Bal(name string) (int, error) {
	txn := db.Txn()
	customerId, err := txn.Get("account:" + name)
	if err != nil {
		return -1, err
	}

	savingBalance, err := txn.GetInt("saving:" + customerId)
	if err != nil {
		return -1, err
	}

	checkingBalance, err := txn.GetInt("checking:" + customerId)
	if err != nil {
		return -1, err
	}

	txn.Commit()

	return savingBalance + checkingBalance, nil
}

// rollback if account do not exists or **value** is negative
func (db *DB) DepositChecking(name string, value int) error {
	txn := db.Txn()

	customerId, err := txn.Get("account:" + name)
	if err != nil {
		return err
	}

	checkingBalance, err := txn.GetInt("checking:" + customerId)
	if err != nil {
		return err
	}

	txn.Commit()

	return txn.UpdateInt(customerId, checkingBalance+value)
}

// rollback if account do not exists or **result** is negative
func (db *DB) TransactSaving(name string, value int) error {
	txn := db.Txn()

	customerId, err := txn.Get("account:" + name)
	if err != nil {
		return err
	}

	checkingBalance, err := txn.GetInt("checking:" + customerId)
	if err != nil {
		return err
	}

	txn.Commit()

	return txn.UpdateInt(customerId, checkingBalance+value)
}

func (db *DB) Amalgamate(from, to string) error {
	txn := db.Txn()

	fromId, err := txn.Get("account:" + from)
	if err != nil {
		return err
	}

	savingBalance, err := txn.GetInt("saving:" + fromId)
	if err != nil {
		return err
	}

	err = txn.UpdateInt("saving:"+fromId, 0)
	if err != nil {
		return err
	}

	checkingBalance, err := txn.GetInt("checking:" + fromId)
	if err != nil {
		return err
	}

	err = txn.UpdateInt("checking:"+fromId, 0)
	if err != nil {
		return err
	}

	toId, err := txn.Get("account:" + to)
	if err != nil {
		return err
	}

	balance, err := txn.GetInt("checking:" + toId)
	if err != nil {
		return err
	}

	err = txn.UpdateInt("checking:"+toId, balance+savingBalance+checkingBalance)
	if err != nil {
		return nil
	}

	txn.Commit()
	return nil
}

func (db *DB) WriteCheck(name string, value int) error {
	txn := db.Txn()
	customerId, err := txn.Get("account:" + name)
	if err != nil {
		return err
	}

	savingBalance, err := txn.GetInt("saving:" + customerId)
	if err != nil {
		return err
	}

	checkingBalance, err := txn.GetInt("checking:" + customerId)
	if err != nil {
		return err
	}

	subtract := value
	if savingBalance+checkingBalance < value {
		subtract += 1
	}

	err = txn.UpdateInt("checking:"+name, checkingBalance-subtract)
	if err != nil {
		return err
	}

	return nil
}
