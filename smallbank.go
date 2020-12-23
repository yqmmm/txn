package txn

import (
	"fmt"
	"math/rand"
)

// Table: account, saving, checking
// https://www.comp.nus.edu.sg/~cs5226/papers/si-cost-icde08.pdf

// TODO: investigate better string concat methods
type SmallBank struct {
	db        Database
	customers []string
	ids       []string
}

type SmallBankConfig struct {
	Customers        int
	HotspotCustomers int  // 90% operation operates on hotspot customers
	UniformOperation bool // false means 60% Bal operation
}

func NewSmallBank(config *SmallBankConfig, db Database) *SmallBank {

	s := &SmallBank{
		db:        db,
		customers: make([]string, 0),
		ids:       make([]string, 0),
	}

	customers := make(map[string]bool)
	ids := make(map[string]bool)
	for i := 0; i < config.Customers; i++ {
		name := randSeq(5)
		for customers[name] {
			name = randSeq(5)
		}
		customers[name] = true

		id := randSeq(8)
		for ids[id] {
			id = randSeq(8)
		}
		ids[id] = true

		// TODO: What number to use? seems like a math problem
		db.Insert("account:"+name, id)
		db.Insert("saving:"+id, "1000")
		db.Insert("checking:"+id, "1000")
	}

	for c := range customers {
		s.customers = append(s.customers, c)
	}

	for id := range ids {
		s.ids = append(s.ids, id)
	}

	return s
}

func (s *SmallBank) Test() error {
	name := s.customers[rand.Intn(len(s.customers))]

	var err error
	switch idx := rand.Intn(5); idx {
	case 0:
		_, err = s.Bal(name)
	case 1:
		err = s.DepositChecking(name, rand.Intn(100))
	case 2:
		err = s.TransactSaving(name, rand.Intn(100))
	case 3:
		anotherName := s.customers[rand.Intn(len(s.customers))]
		err = s.Amalgamate(name, anotherName)
	case 4:
		err = s.WriteCheck(name, rand.Intn(100))
	}

	return err
}

// TODO: record value and check correctness, not when benchmarking
func (s *SmallBank) Check() error {
	expected := len(s.customers) * 2000
	total := 0

	txn := s.db.Txn()
	for _, c := range s.ids {
		v, err := txn.GetInt("saving:" + c)
		if err != nil {
			fmt.Println(err)
		}
		total += v

		v, err = txn.GetInt("checking:" + c)
		if err != nil {
			fmt.Println(err)
		}
		total += v
	}

	if expected == total {
		fmt.Println("Right!")
	} else {
		fmt.Printf("Wrong! expected :%d, got %d\n", expected, total)
	}

	return nil
}

func (s *SmallBank) Bal(name string) (int, error) {
	txn := s.db.Txn()
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
func (s *SmallBank) DepositChecking(name string, value int) error {
	txn := s.db.Txn()

	customerId, err := txn.Get("account:" + name)
	if err != nil {
		return err
	}

	checkingBalance, err := txn.GetInt("checking:" + customerId)
	if err != nil {
		return err
	}

	err = txn.UpdateInt("checking:"+customerId, checkingBalance+value)
	if err != nil {
		return err
	}

	txn.Commit()
	return nil
}

// rollback if account do not exists or **result** is negative
func (s *SmallBank) TransactSaving(name string, value int) error {
	txn := s.db.Txn()

	customerId, err := txn.Get("account:" + name)
	if err != nil {
		return err
	}

	checkingBalance, err := txn.GetInt("checking:" + customerId)
	if err != nil {
		return err
	}

	err = txn.UpdateInt("saving:"+customerId, checkingBalance+value)
	if err != nil {
		return err
	}

	txn.Commit()
	return nil
}

func (s *SmallBank) Amalgamate(from, to string) error {
	txn := s.db.Txn()

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

func (s *SmallBank) WriteCheck(name string, value int) error {
	txn := s.db.Txn()
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

	err = txn.UpdateInt("checking:"+customerId, checkingBalance-subtract)
	if err != nil {
		return err
	}

	txn.Commit()
	return nil
}

func (s *SmallBank) handleError(err error) {
	e, ok := err.(AbortError)
	if ok {
		fmt.Println(e)
	}
}
