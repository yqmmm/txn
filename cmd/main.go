package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/yqmmm/txn"
)

func main() {
	config := &txn.SmallBankConfig{
		Customers:        100,
		HotspotCustomers: 10,
		Concurrency:      8,
		Timeout:          10 * time.Second,
	}

	flag.IntVar(&config.Customers, "customers", 1800, "Number of customers")
	flag.IntVar(&config.Customers, "hot", 30, "Number of hot customers")
	waitDie := flag.Bool("wait-die", true, "wait-die, or else wound-wait")

	flag.Parse()

	var lockType string
	if *waitDie {
		lockType = txn.WaitDie
	} else {
		lockType = txn.WoundWait
	}

	success, failure := txn.Benchmark(config, lockType)
	fmt.Printf("Success:%v\nFailure:%v\n", success, failure)
}
