package thunder

import (
	"bytes"
	"iter"

	"github.com/openkvlab/boltdb"
)

type dataStorage struct {
	bucket *boltdb.Bucket
	maUn   MarshalUnmarshaler
}

func newData(
	parentBucket *boltdb.Bucket,
	maUn MarshalUnmarshaler,
) (*dataStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("data"))
	if err != nil {
		return nil, err
	}
	return &dataStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func loadData(
	parentBucket *boltdb.Bucket,
	maUn MarshalUnmarshaler,
) (*dataStorage, error) {
	bucket := parentBucket.Bucket([]byte("data"))
	if bucket == nil {
		return nil, nil
	}
	return &dataStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func (d *dataStorage) insert(value any) ([]byte, error) {
	id, err := d.bucket.NextSequence()
	if err != nil {
		return nil, err
	}
	idBytes, err := orderedMa.Marshal(id)
	if err != nil {
		return nil, err
	}
	dataBytes, err := d.maUn.Marshal(value)
	if err != nil {
		return nil, err
	}
	return idBytes, d.bucket.Put(idBytes, dataBytes)
}

func (d *dataStorage) get(kr *keyRange) (iter.Seq2[entry, error], error) {
	return func(yield func(entry, error) bool) {
		c := d.bucket.Cursor()
		lessThan := func(k []byte) bool {
			if kr.endKey == nil {
				return true
			}
			cmp := bytes.Compare(k, kr.endKey)
			return cmp < 0 || (cmp == 0 && kr.includeEnd)
		}
		var k, v []byte
		if kr.startKey != nil {
			k, v = c.Seek(kr.startKey)
		} else {
			k, v = c.First()
		}
		if !kr.includeStart {
			k, v = c.Next()
		}
		for ; k != nil && lessThan(k); k, v = c.Next() {
			if !kr.contains(k) {
				continue
			}
			var value map[string]any
			if err := d.maUn.Unmarshal(v, &value); err != nil {
				if !yield(entry{}, err) {
					return
				}
				continue
			}
			if !yield(entry{
				id:    k,
				value: value,
			}, nil) {
				return
			}
		}
	}, nil
}

func (d *dataStorage) delete(id []byte) error {
	return d.bucket.Delete(id)
}

type entry struct {
	id    []byte
	value map[string]any
}
