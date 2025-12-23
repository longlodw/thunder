package thunder

import (
	"bytes"
	"iter"

	"github.com/openkvlab/boltdb"
)

type indexStorage struct {
	bucket *boltdb.Bucket
	maUn   MarshalUnmarshaler
}

func newIndex(
	parentBucket *boltdb.Bucket,
	idxNames []string,
	maUn MarshalUnmarshaler,
) (*indexStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("indexes"))
	if err != nil {
		return nil, err
	}
	for _, name := range idxNames {
		_, err := bucket.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return nil, err
		}
	}
	return &indexStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func loadIndex(
	parentBucket *boltdb.Bucket,
	maUn MarshalUnmarshaler,
) (*indexStorage, error) {
	bucket := parentBucket.Bucket([]byte("indexes"))
	if bucket == nil {
		return nil, nil
	}
	return &indexStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func (idx *indexStorage) insert(name string, idxLoc *indexLocator) error {
	key := idxLoc.Key
	id := idxLoc.Id
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return ErrIndexNotFound(name)
	}
	compositeKey, err := orderedMa.Marshal([]any{key, id})
	if err != nil {
		return err
	}
	return indexBk.Put(compositeKey, nil)
}

func (idx *indexStorage) delete(name string, idxLoc *indexLocator) error {
	key := idxLoc.Key
	id := idxLoc.Id
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return ErrIndexNotFound(name)
	}
	compositeKey, err := orderedMa.Marshal([]any{key, id})
	if err != nil {
		return err
	}
	return indexBk.Delete(compositeKey)
}

type indexLocator struct {
	Key []byte `json:"key"`
	Id  uint64 `json:"id"`
}

func (idx *indexStorage) get(name string, kr *keyRange) (iter.Seq2[uint64, error], error) {
	idxBk := idx.bucket.Bucket([]byte(name))
	if idxBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	return func(yield func(uint64, error) bool) {
		c := idxBk.Cursor()
		var k []byte
		var seekPrefix []byte
		var err error

		if kr.startKey != nil {
			seekPrefix, err = orderedMa.Marshal([]any{kr.startKey})
			if err != nil {
				if !yield(0, err) {
					return
				}
				return
			}
			k, _ = c.Seek(seekPrefix)
		} else {
			k, _ = c.First()
		}

		lessThanEnd := func(k []byte) bool {
			if kr.endKey == nil {
				return true
			}
			cmpEnd := bytes.Compare(k, kr.endKey)
			return cmpEnd < 0 || (cmpEnd == 0 && kr.includeEnd)
		}

		for ; k != nil; k, _ = c.Next() {
			var parts []any
			if err := orderedMa.Unmarshal(k, &parts); err != nil {
				if !yield(0, err) {
					return
				}
				continue
			}

			if len(parts) != 2 {
				continue
			}

			var valBytes []byte
			switch v := parts[0].(type) {
			case []byte:
				valBytes = v
			case string:
				valBytes = []byte(v)
			default:
				continue
			}

			idAny := parts[1]

			var id uint64
			switch v := idAny.(type) {
			case uint64:
				id = v
			case int64:
				id = uint64(v)
			case int:
				id = uint64(v)
			default:
				continue
			}

			if !kr.contains(valBytes) {
				// We need to check if we should stop or just skip.
				// Since we are iterating in order:
				// If valBytes > endKey, we can stop.
				// But we need to check strict inequality for endKey.

				if !lessThanEnd(valBytes) {
					break
				}

				// If we are here, it means kr.contains returned false,
				// but we are still <= endKey.
				// This implies we are < startKey (shouldn't happen with Seek unless we wrapped around? No)
				// OR !includeStart/!includeEnd boundary cases.
				// OR excluded.

				continue
			}

			if !yield(id, nil) {
				return
			}
		}
	}, nil
}
