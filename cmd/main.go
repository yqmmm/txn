package main

import (
	"github.com/yqmmm/txn"
)

func main() {
	config := &txn.SmallBankConfig{
		Customers:        100,
		HotspotCustomers: 10,
		UniformOperation: true,
	}

	s := txn.NewSmallBank(config)

	s.Test()
}
