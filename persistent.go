package thunder

import (
	"bytes"
	"cmp"
	"iter"
	"maps"
	"reflect"
	"slices"
)

// Persistent represents an object relation in the database.
type Persistent struct {
	data        *dataStorage
	indexes     *indexStorage
	reverseIdx  *reverseIndexStorage
	indexesMeta map[string][]string
	uniquesMeta map[string][]string
	columns     []string
	relation    string
	allIndexes  []string
}

func (pr *Persistent) Insert(obj map[string]any) error {
	if len(obj) != len(pr.columns) {
		return ErrObjectFieldCountMismatch
	}
	for _, col := range pr.columns {
		if _, ok := obj[col]; !ok {
			return ErrObjectMissingField(col)
		}
	}
	id, err := pr.data.insert(obj)
	if err != nil {
		return err
	}
	// Check uniques
	for uniqueName, keyFields := range pr.uniquesMeta {
		keyParts := make([]any, len(keyFields))
		for i, kf := range keyFields {
			keyParts[i] = obj[kf]
		}
		idxRanges, err := toRanges(Eq(uniqueName, keyParts))
		if err != nil {
			return err
		}
		idxRange := idxRanges[uniqueName]
		exists, err := pr.indexes.get(uniqueName, idxRange)
		if err != nil {
			return err
		}
		for range exists {
			return ErrUniqueConstraint(uniqueName)
		}
	}

	// Update indexes
	revIdx := make(map[string][]byte)
	for idxName, keyFields := range pr.indexesMeta {
		keyParts := make([]any, len(keyFields))
		for i, kf := range keyFields {
			keyParts[i] = obj[kf]
		}
		revIdxField, err := pr.indexes.insert(idxName, keyParts, id)
		if err != nil {
			return err
		}
		revIdx[idxName] = revIdxField
	}
	for idxName, keyFields := range pr.uniquesMeta {
		keyParts := make([]any, len(keyFields))
		for i, kf := range keyFields {
			keyParts[i] = obj[kf]
		}
		revIdxField, err := pr.indexes.insert(idxName, keyParts, id)
		if err != nil {
			return err
		}
		revIdx[idxName] = revIdxField
	}
	if err := pr.reverseIdx.insert(id, revIdx); err != nil {
		return err
	}
	return nil
}

func (pr *Persistent) Delete(ops ...Op) error {
	iterEntries, err := pr.iter(ops...)
	if err != nil {
		return err
	}
	for e, err := range iterEntries {
		if err != nil {
			return err
		}
		// Delete from indexes
		revIdx, err := pr.reverseIdx.get(e.id)
		if err != nil {
			return err
		}
		for idxName, revIdxField := range revIdx {
			keyFields, ok := pr.indexesMeta[idxName]
			if !ok {
				return ErrIndexMetadataNotFound(idxName)
			}
			keyParts := make([]any, len(keyFields))
			for i, kf := range keyFields {
				keyParts[i] = e.value[kf]
			}
			if err := pr.indexes.delete(idxName, keyParts, revIdxField); err != nil {
				return err
			}
		}
		if err := pr.reverseIdx.delete(e.id); err != nil {
			return err
		}
		// Delete from data
		if err := pr.data.delete(e.id); err != nil {
			return err
		}
	}
	return nil
}

func (pr *Persistent) Select(ops ...Op) (iter.Seq2[map[string]any, error], error) {
	iterEntries, err := pr.iter(ops...)
	if err != nil {
		return nil, err
	}
	return func(yield func(map[string]any, error) bool) {
		iterEntries(func(e entry, err error) bool {
			if err != nil {
				return yield(nil, err)
			}
			return yield(e.value, nil)
		})
	}, nil
}

func (pr *Persistent) Name() string {
	return pr.relation
}

func (pr *Persistent) Columns() []string {
	return slices.Clone(pr.columns)
}

func (pr *Persistent) Project(mapping map[string]string) (Selector, error) {
	return newProjection(pr, mapping)
}

func (pr *Persistent) iter(ops ...Op) (iter.Seq2[entry, error], error) {
	ranges, err := toRanges(ops...)
	if err != nil {
		return nil, err
	}
	selectedIndexes := make([]string, 0, len(ranges))
	for _, idxName := range pr.allIndexes {
		if _, ok := ranges[idxName]; ok {
			selectedIndexes = append(selectedIndexes, idxName)
		}
	}
	if len(selectedIndexes) == 0 {
		// No indexes defined, full scan
		entries, err := pr.data.get(&keyRange{
			includeEnd:   true,
			includeStart: true,
		})
		if err != nil {
			return nil, err
		}
		return func(yield func(entry, error) bool) {
			for e, err := range entries {
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				matches, err := pr.matchOps(e.value, ranges)
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				if matches && !yield(e, nil) {
					return
				}
			}
		}, nil
	}
	shortestRangeIdxName := slices.MinFunc(selectedIndexes, func(a, b string) int {
		distA := ranges[a].distance()
		distB := ranges[b].distance()
		return bytes.Compare(distA, distB)
	})
	rangeIdx := ranges[shortestRangeIdxName]
	idxes, err := pr.indexes.get(shortestRangeIdxName, rangeIdx)
	if err != nil {
		return nil, err
	}
	return func(yield func(entry, error) bool) {
		for idBytes := range idxes {
			id := idBytes
			values, err := pr.data.get(&keyRange{
				includeEnd:   true,
				includeStart: true,
				startKey:     id,
				endKey:       id,
			})
			if err != nil {
				if !yield(entry{}, err) {
					return
				}
				continue
			}
			for e, err := range values {
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				// Match other ops
				matches, err := pr.matchOps(e.value, ranges)
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				if matches && !yield(e, nil) {
					return
				}
			}
		}
	}, nil
}

func (pr *Persistent) matchOps(value map[string]any, keyRanges map[string]*keyRange) (bool, error) {
	compositeValue := maps.Clone(value)
	for k := range keyRanges {
		_, ok := value[k]
		if ok {
			continue
		}
		if cols, ok := pr.indexesMeta[k]; ok {
			parts := make([]any, len(cols))
			for i, col := range cols {
				part, ok := value[col]
				if !ok {
					return false, ErrObjectMissingField(col)
				}
				parts[i] = part
			}
			compositeValue[k] = parts
		} else if cols, ok := pr.uniquesMeta[k]; ok {
			parts := make([]any, len(cols))
			for i, col := range cols {
				part, ok := value[col]
				if !ok {
					return false, ErrObjectMissingField(col)
				}
				parts[i] = part
			}
			compositeValue[k] = parts
		} else {
			return false, ErrFieldNotFoundInColumns(k)
		}
	}
	for name, r := range keyRanges {
		v, ok := compositeValue[name]
		if !ok {
			return false, ErrObjectMissingField(name)
		}
		vBytes, err := orderedMa.Marshal(v)
		if err != nil {
			return false, err
		}
		if !r.contains(vBytes) {
			return false, nil
		}
	}
	return true, nil
}

func apply(value any, o Op) (bool, error) {
	if reflect.TypeOf(value) != reflect.TypeOf(o.Value) {
		return false, ErrTypeMismatch(value, o.Value)
	}
	v, err := compare(value, o.Value)
	if err != nil {
		return false, err
	}
	switch o.Type {
	case OpEq:
		return v == 0, nil
	case OpNe:
		return v != 0, nil
	case OpGt:
		return v > 0, nil
	case OpLt:
		return v < 0, nil
	case OpGe:
		return v >= 0, nil
	case OpLe:
		return v <= 0, nil
	default:
		return false, ErrUnsupportedOperator(o.Type)
	}
}

func compare(a, b any) (int, error) {
	switch va := a.(type) {
	case int:
		return cmp.Compare(va, b.(int)), nil
	case int8:
		return cmp.Compare(va, b.(int8)), nil
	case int16:
		return cmp.Compare(va, b.(int16)), nil
	case int32:
		return cmp.Compare(va, b.(int32)), nil
	case int64:
		return cmp.Compare(va, b.(int64)), nil
	case uint:
		return cmp.Compare(va, b.(uint)), nil
	case uint8:
		return cmp.Compare(va, b.(uint8)), nil
	case uint16:
		return cmp.Compare(va, b.(uint16)), nil
	case uint32:
		return cmp.Compare(va, b.(uint32)), nil
	case uint64:
		return cmp.Compare(va, b.(uint64)), nil
	case uintptr:
		return cmp.Compare(va, b.(uintptr)), nil
	case float32:
		return cmp.Compare(va, b.(float32)), nil
	case float64:
		return cmp.Compare(va, b.(float64)), nil
	case string:
		return cmp.Compare(va, b.(string)), nil
	case []any:
		ba, err := orderedMa.Marshal(a)
		if err != nil {
			return 0, err
		}
		bb, err := orderedMa.Marshal(b)
		if err != nil {
			return 0, err
		}
		return bytes.Compare(ba, bb), nil
	default:
		return 0, ErrUnsupportedType(a)
	}
}
