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

	s := txn.NewSmallBank(config)

	stopChan := make(chan struct{})
	resultChan := make(chan int)

	for i := 0; i < concurrency; i++ {
		go func() {
			count := 0
			for {
				select {
				case <-stopChan:
					resultChan <- count
					return

				default:
					err := s.Test()
					if err != nil {
						fmt.Println(err)
						resultChan <- count
						return
					}
					count++
				}
			}
		}()
	}

	time.Sleep(timeout)
	close(stopChan)

	result := 0
	for i := 0; i < concurrency; i++ {
		result += <-resultChan
	}

	fmt.Printf("Op: %v\n", result)
}
