package sequel

import (
	"reflect"
)

func (b *builder) addErr(err error) {
	if err == nil {
		return
	}

	if b.err == nil {
		b.err = err
	}
}

func flattenArgs(values []any) ([]any, error) {
	if values == nil {
		return []any{}, nil
	}

	args := make([]any, 0, len(values))

	for _, v := range values {
		if v == nil {
			return nil, ErrNilNotAllowed
		}

		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			for i := 0; i < rv.Len(); i++ {
				elem := rv.Index(i).Interface()
				if elem == nil {
					return nil, ErrNilNotAllowed
				}

				ev := reflect.ValueOf(elem)
				if ev.Kind() == reflect.Slice || ev.Kind() == reflect.Array {
					return nil, ErrNestedSlice
				}

				args = append(args, elem)
			}
		default:
			args = append(args, v)
		}
	}

	return args, nil
}
