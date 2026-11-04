package py

import (
	"math/big"
	"sort"
)

// Lt is Python's a < b for the value model: numbers with numbers, str with
// str, bytes with bytes, tuples/lists lexicographically; anything else is
// TypeError("'<' not supported between instances of 'X' and 'Y'").
func Lt(a, b any) (bool, error) {
	if fa, ok := toFloatOrBig(a); ok {
		if fb, ok := toFloatOrBig(b); ok {
			return fa.Cmp(fb) < 0, nil
		}
	}
	switch x := a.(type) {
	case string:
		if y, ok := b.(string); ok {
			return x < y, nil
		}
	case []byte:
		if y, ok := b.([]byte); ok {
			return string(x) < string(y), nil
		}
	case Tuple:
		if y, ok := b.(Tuple); ok {
			return seqLt([]any(x), []any(y))
		}
	case *List:
		if y, ok := b.(*List); ok {
			return seqLt(x.Items(), y.Items())
		}
	case interface{ Lt(other any) (bool, error) }:
		return x.Lt(b)
	}
	return false, TypeError.Newf("'<' not supported between instances of '%s' and '%s'", Pytypename(a), Pytypename(b))
}

// Neg is Python's unary ``-a`` for the numeric value model: ints stay
// ints, floats stay floats; TypeError("bad operand type for unary -: 'X'")
// otherwise.
func Neg(a any) (any, error) {
	if !isNumber(a) {
		return nil, TypeError.Newf("bad operand type for unary -: '%s'", Pytypename(a))
	}
	if isFloat(a) {
		f, _ := Float(a)
		return -f, nil
	}
	i, _ := toFloatOrBigInt(a)
	neg := new(big.Int).Neg(i)
	if neg.IsInt64() {
		return int(neg.Int64()), nil
	}
	return neg, nil
}

// Add is Python's a + b for the value model: int + int stays int
// (promoting to *big.Int on overflow), any float operand gives a float,
// str + str concatenates, list/tuple + list/tuple concatenates; otherwise
// TypeError("unsupported operand type(s) for +: 'X' and 'Y'").
func Add(a, b any) (any, error) {
	if isNumber(a) && isNumber(b) {
		if isFloat(a) || isFloat(b) {
			fa, _ := Float(a)
			fb, _ := Float(b)
			return fa + fb, nil
		}
		ia, _ := toFloatOrBigInt(a)
		ib, _ := toFloatOrBigInt(b)
		sum := new(big.Int).Add(ia, ib)
		if sum.IsInt64() {
			return int(sum.Int64()), nil
		}
		return sum, nil
	}
	switch x := a.(type) {
	case string:
		if y, ok := b.(string); ok {
			return x + y, nil
		}
	case *List:
		if y, ok := b.(*List); ok {
			return NewList(append(x.Items(), y.Items()...)...), nil
		}
	case []any:
		if y, ok := b.([]any); ok {
			return append(append([]any(nil), x...), y...), nil
		}
	case Tuple:
		if y, ok := b.(Tuple); ok {
			return Tuple(append(append([]any(nil), x...), y...)), nil
		}
	case interface{ Add(other any) (any, error) }:
		return x.Add(b)
	}
	return nil, TypeError.Newf("unsupported operand type(s) for +: '%s' and '%s'", Pytypename(a), Pytypename(b))
}

func isFloat(v any) bool {
	switch v.(type) {
	case float32, float64:
		return true
	}
	return false
}

func isNumber(v any) bool {
	switch v.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, *big.Int:
		return true
	}
	return false
}

// toFloatOrBigInt gives integral values a common exact form.
func toFloatOrBigInt(v any) (*big.Int, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return big.NewInt(1), true
		}
		return big.NewInt(0), true
	case *big.Int:
		return x, true
	case uint64:
		return new(big.Int).SetUint64(x), true
	}
	if k, err := keyOf(v); err == nil && k.kind == hkInt {
		return big.NewInt(k.i), true
	}
	return nil, false
}

func seqLt(x, y []any) (bool, error) {
	for i := 0; i < len(x) && i < len(y); i++ {
		if Eq(x[i], y[i]) {
			continue
		}
		return Lt(x[i], y[i])
	}
	return len(x) < len(y), nil
}

// toFloatOrBig gives numbers a common comparable form (big.Float keeps
// int64/uint64/big.Int exact; floats compare by value).
func toFloatOrBig(v any) (*big.Float, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return big.NewFloat(1), true
		}
		return big.NewFloat(0), true
	case float32:
		return big.NewFloat(float64(x)), true
	case float64:
		if x != x { // NaN compares false with everything; treat as incomparable-but-typed
			return big.NewFloat(0), true
		}
		return big.NewFloat(x), true
	case *big.Int:
		return new(big.Float).SetInt(x), true
	case uint64:
		return new(big.Float).SetUint64(x), true
	}
	if k, err := keyOf(v); err == nil && k.kind == hkInt {
		return new(big.Float).SetInt64(k.i), true
	}
	return nil, false
}

// Sorted_index returns the permutation that sorts keys with Python's
// sorted() semantics; incomparable neighbours raise the TypeError CPython
// would raise (its run detection compares a[i] < a[i-1] left to right).
func Sorted_index(keys []any) ([]int, error) {
	n := len(keys)
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	for i := 1; i < n; i++ {
		if _, err := Lt(keys[i], keys[i-1]); err != nil {
			return nil, err
		}
	}
	var sortErr error
	sort.SliceStable(idx, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		lt, err := Lt(keys[idx[i]], keys[idx[j]])
		if err != nil {
			sortErr = err
			return false
		}
		return lt
	})
	if sortErr != nil {
		return nil, sortErr
	}
	return idx, nil
}

// Sorted is sorted(items) for the value model.
func Sorted(items []any) ([]any, error) {
	order, err := Sorted_index(items)
	if err != nil {
		return nil, err
	}
	out := make([]any, len(items))
	for i, j := range order {
		out[i] = items[j]
	}
	return out, nil
}

// Sorted_strings sorts strings (sorted(list_of_str)).
func Sorted_strings(items []string) []string {
	out := append([]string(nil), items...)
	sort.Strings(out)
	return out
}
