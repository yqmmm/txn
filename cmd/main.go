package main

import (
	"fmt"
	"sync"

	"github.com/yqmmm/txn"
)

func main() {
	config := &txn.SmallBankConfig{
		Customers:        1000,
		HotspotCustomers: 10,
		UniformOperation: true,
	}

	s := txn.NewSmallBank(config)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 1000; i++ {
				err := s.Test()
				if err != nil {
					fmt.Println(err)
					return
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
