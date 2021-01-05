package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/yqmmm/txn"
)

func main() {
	lockType := txn.WaitDie
	config := txn.SmallBankConfig{
		Customers:        1800,
		HotspotCustomers: 100,
		Concurrency:      8,
		Timeout:          10 * time.Second,
	}
	var db *txn.LockDB
	switch lockType {
	case txn.WaitDie:
		db = txn.NewLockDB(txn.NewWaitDieLock)
	case txn.WoundWait:
		db = txn.NewLockDB(txn.NewWoundWaitLock)
	}

	f, err := os.OpenFile("result.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	w := csv.NewWriter(f)
	//w.Write([]string{"type", "customers", "hot", "concurrency", "timeout", "success", "failure"})

	succ, fail := txn.Benchmark(&config, db)

	w.Write([]string{
		lockType,
		strconv.Itoa(config.Customers),
		strconv.Itoa(config.HotspotCustomers),
		strconv.Itoa(config.Concurrency),
		fmt.Sprintf("%f", config.Timeout.Seconds()),
		strconv.Itoa(succ),
		strconv.Itoa(fail),
	})

	w.Flush()
	f.Close()
}
