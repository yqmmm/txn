package main

import (
	"fmt"
	"time"

	"github.com/yqmmm/txn"
)

func main() {
	concurrency := 8
	timeout := 10 * time.Second

	config := &txn.SmallBankConfig{
		Customers:        1000,
		HotspotCustomers: 10,
		UniformOperation: true,
	}

	db := txn.NewLockDB(txn.NewWaitDieLock)

	s := txn.NewSmallBank(config, db)

	stopChan := make(chan struct{})
	type Result struct {
		success int
		failure int
	}
	resultChan := make(chan Result)

	for i := 0; i < concurrency; i++ {
		go func() {
			success, failure := 0, 0
			for {
				select {
				case <-stopChan:
					resultChan <- Result{
						success: success,
						failure: failure,
					}
					return

				default:
					err := s.Test()
					if err != nil {
						failure++
					} else {
						success++
					}
				}
			}
		}()
	}

	time.Sleep(timeout)
	close(stopChan)

	success, failure := 0, 0
	for i := 0; i < concurrency; i++ {
		result := <-resultChan
		success += result.success
		failure += result.failure
	}

	fmt.Printf("Success: %v\nFailure: %v\n", success, failure)
}
