package txn

type Database interface {
	Txn() Txn
	Insert(k, v string) // Need not transaction
}

type Txn interface {
	Commit() error
	Get(k string) (string, error)
	GetInt(k string) (int, error)
	Update(k, v string) error
	UpdateInt(k string, v int) error
}
