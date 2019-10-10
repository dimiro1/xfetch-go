package xf

import (
	"reflect"

	"github.com/pkg/errors"
)

func assign(dest, src interface{}) error {
	destValue := reflect.ValueOf(dest)

	srcValue := reflect.ValueOf(src)
	if srcValue.Kind() != reflect.Ptr || srcValue.IsNil() {
		return errors.New("recomputed's underlying value type not be a non-nil pointer")
	}

	fetchableType := destValue.Type()
	recomputedType := srcValue.Type()
	if fetchableType != recomputedType {
		return errors.Errorf("fetchable type %s is not assignable to recomputed type %s", fetchableType, recomputedType)
	}
	destValue.Elem().Set(srcValue.Elem())
	return nil
}
