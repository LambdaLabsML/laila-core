package py

import "iter"

// Iterator is a lazy Python generator in Go form: ``for v, err := range
// it`` pulls one item per step; a non-nil err is the exception the
// generator raised at that point (iteration stops after it).
type Iterator = iter.Seq2[any, error]

// Iterator_of builds an Iterator over a fixed slice (``iter(list)``).
func Iterator_of(items []any) Iterator {
	return func(yield func(any, error) bool) {
		for _, it := range items {
			if !yield(it, nil) {
				return
			}
		}
	}
}

// Collect is ``list(it)``: drains the iterator, stopping at the first
// error.
func Collect(it Iterator) ([]any, error) {
	out := []any{}
	for v, err := range it {
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}
