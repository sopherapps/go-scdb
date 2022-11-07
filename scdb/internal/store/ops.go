package store

// OpType is a type of Op that can be done on Store
type OpType uint

const (
	// SetOp for setting key-value pairs
	SetOp OpType = iota
	// GetOp for getting key-value pairs
	GetOp
	// DeleteOp for deleting key-value pairs
	DeleteOp
	// ClearOp for clearing the Store
	ClearOp
	// CompactOp for compacting the Store
	CompactOp
	// CloseOp for closing the Store
	CloseOp
	// GetStoreOp for getting the current Store's instance
	GetStoreOp
)

// Op is an Operation that is to be done on a Store
type Op struct {
	Type     OpType
	Key      []byte
	Value    []byte
	Ttl      *uint64
	RespChan chan OpResult
}

// OpResult is the result of an Op done on the Store
// It is usually pushed to the Op.RespChan so that
// the sender of the Op can receive the result and act upon it
type OpResult struct {
	Err   error
	Value []byte
	Store *Store
}
