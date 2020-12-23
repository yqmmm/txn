package txn

type NotFoundError struct {
	key string
}

func (e NotFoundError) Error() string {
	return "Not Found: " + e.key
}

type DeadLockError struct {
	key string
}

func (e DeadLockError) Error() string {
	return "Dead Lock: " + e.key
}
