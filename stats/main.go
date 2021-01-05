package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/yqmmm/txn"
)

var hot = []int{10, 50, 100, 200, 500, 1000}

func main() {
	//lockType := txn.WaitDie
	lockType := txn.WoundWait
	config := txn.SmallBankConfig{
		Customers:        18000,
		HotspotCustomers: 100,
		Concurrency:      8,
		Timeout:          10 * time.Second,
	}

	f, err := os.OpenFile("results/results.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	w := csv.NewWriter(f)
	//w.Write([]string{"type", "customers", "hot", "concurrency", "timeout", "success", "failure"})

	for _, h := range hot {
		config.HotspotCustomers = h
		succ, fail := txn.Benchmark(&config, lockType)

		w.Write([]string{
			lockType,
			strconv.Itoa(config.Customers),
			strconv.Itoa(config.HotspotCustomers),
			strconv.Itoa(config.Concurrency),
			fmt.Sprintf("%f", config.Timeout.Seconds()),
			strconv.Itoa(succ),
			strconv.Itoa(fail),
		})
	}

	w.Flush()
	f.Close()
}
