package store

import (
	"encoding/binary"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

var bucketLogs = []byte("logs")

// BoltLogStore implements raft.LogStore using BoltDB.
type BoltLogStore struct {
	db *bolt.DB
}

func NewBoltLogStore(path string) (*BoltLogStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketLogs)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltLogStore{db: db}, nil
}

func (s *BoltLogStore) Close() error {
	return s.db.Close()
}

func (s *BoltLogStore) FirstIndex() (uint64, error) {
	var idx uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLogs)
		k, _ := b.Cursor().First()
		if k != nil {
			idx = bytesToUint64(k)
		}
		return nil
	})
	return idx, err
}

func (s *BoltLogStore) LastIndex() (uint64, error) {
	var idx uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLogs)
		k, _ := b.Cursor().Last()
		if k != nil {
			idx = bytesToUint64(k)
		}
		return nil
	})
	return idx, err
}

func (s *BoltLogStore) GetLog(idx uint64, out *raft.Log) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLogs)
		v := b.Get(uint64ToBytes(idx))
		if v == nil {
			return raft.ErrLogNotFound
		}
		return decodeMsgPack(v, out)
	})
}

func (s *BoltLogStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *BoltLogStore) StoreLogs(logs []*raft.Log) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLogs)
		for _, log := range logs {
			key := uint64ToBytes(log.Index)
			val, err := encodeMsgPack(log)
			if err != nil {
				return err
			}
			if err := b.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltLogStore) DeleteRange(min, max uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLogs)
		c := b.Cursor()
		for k, _ := c.Seek(uint64ToBytes(min)); k != nil; k, _ = c.Next() {
			if bytesToUint64(k) > max {
				break
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func uint64ToBytes(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
