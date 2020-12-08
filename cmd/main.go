package main

import (
	"fmt"

	"github.com/yqmmm/txn"
)

func main() {
	config := &txn.SmallBankConfig{
		Customers:        100,
		HotspotCustomers: 10,
		UniformOperation: true,
	}

	s := txn.NewSmallBank(config)

	for i := 0; i < 1000; i++ {
		err := s.Test()
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
