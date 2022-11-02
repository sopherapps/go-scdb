package scdb

type Store struct {
}

// New creates a new Store at the given path
func New(path string, maxKeys *uint64, redundantBlocks *uint16, poolCapacity *uint, compactionInterval *uint32) (*Store, error) {
	poolCap := uint(5)
	if poolCapacity != nil {
		poolCap = *poolCapacity
	}

	maxKeyNum := uint64(1_000_000)
	if maxKeys != nil {
		maxKeyNum = *maxKeys
	}

	redundantBlocksNum := uint16(1)
	if redundantBlocks != nil {
		redundantBlocksNum = *redundantBlocks
	}

	compactionIntervalNum := uint32(3_600)
	if compactionInterval != nil {
		compactionIntervalNum = *compactionInterval
	}

	println(compactionIntervalNum, redundantBlocksNum, maxKeyNum, poolCap)
	panic("implement me")
}

func (s *Store) Set(k []byte, v []byte, ttl *uint64) error {
	//TODO implement me
	panic("implement me")
}

func (s *Store) Get(k []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (s *Store) Delete(k []byte) error {
	//TODO implement me
	panic("implement me")
}

func (s *Store) Clear() error {
	//TODO implement me
	panic("implement me")
}

func (s *Store) Compact() error {
	//TODO implement me
	panic("implement me")
}
