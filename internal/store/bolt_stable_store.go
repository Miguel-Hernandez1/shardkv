package store

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

var bucketConf = []byte("conf")

// BoltStableStore implements raft.StableStore using BoltDB.
type BoltStableStore struct {
	db *bolt.DB
}

func NewBoltStableStore(path string) (*BoltStableStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketConf)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltStableStore{db: db}, nil
}

func (s *BoltStableStore) Close() error {
	return s.db.Close()
}

func (s *BoltStableStore) Set(key []byte, val []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketConf).Put(key, val)
	})
}

func (s *BoltStableStore) Get(key []byte) ([]byte, error) {
	var val []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketConf).Get(key)
		if v == nil {
			return errors.New("not found")
		}
		val = make([]byte, len(v))
		copy(val, v)
		return nil
	})
	return val, err
}

func (s *BoltStableStore) SetUint64(key []byte, val uint64) error {
	return s.Set(key, uint64ToBytes(val))
}

func (s *BoltStableStore) GetUint64(key []byte) (uint64, error) {
	val, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}
