package config

import (
	"fmt"
	"reflect"
)

// overlay overwrites the bottom struct with nonzero values from the top struct,
// using reflection to iterate the struct.
// Both objects MUST be of the same base type, which also must be a struct.
// The bottom object is modified in place and must be a pointer to a struct.
// The top object is not modified and may be a struct or a pointer to a struct.
// It only merges the leaf nodes.
// If a key is present in both objects as a basic type, the value from the
// top object is used only if it is non-zero.
// If an object is a nested struct, the nested struct is recursively merged.
// If an object is a slice, the slices are concatenated, with top's slice last.
// If an object is a map, the maps are merged, with top's map last.
func overlay(bottom, top reflect.Value) (reflect.Value, error) {
	// fail if top is bad
	switch top.Kind() {
	default:
		// it's not a struct, it's bad
		return bottom, fmt.Errorf("overlay: unsupported type %v", top.Kind())
	case reflect.Pointer:
	case reflect.Struct:
	}

	switch bottom.Kind() {
	default:
		// it's not a struct, it's bad
		return bottom, fmt.Errorf("overlay: unsupported type %v", bottom.Kind())
	case reflect.Pointer:
		// dereference the pointers
		if bottom.IsNil() {
			return top, nil
		}
		var e error
		if top.Kind() == reflect.Ptr {
			_, e = overlay(bottom.Elem(), top.Elem())
		} else {
			_, e = overlay(bottom.Elem(), top)
		}
		// return the original pointer, not the modified value
		return bottom, e
	case reflect.Struct:
		bt := bottom.Type()

		for i := 0; i < bottom.NumField(); i++ {
			field := bottom.Field(i)
			fieldType := bt.Field(i)
			switch fieldType.Type.Kind() {
			case reflect.Struct:
				// recurse into any nested structs
				topField := top.Field(i)
				merged, err := overlay(field, topField)
				if err != nil {
					return bottom, err
				}
				if merged.Kind() == reflect.Ptr {
					field.Set(merged.Elem())
				} else {
					field.Set(merged)
				}
			case reflect.Slice:
				// concatenate slices
				if field.CanAddr() {
					topField := top.Field(i)
					if topField.CanAddr() {
						merged := reflect.AppendSlice(field, topField)
						field.Set(merged)
					}
				}
			case reflect.Map:
				// merge maps
				if field.CanAddr() {
					topField := top.Field(i)
					if topField.CanAddr() {
						merged := reflect.MakeMap(field.Type())
						for _, key := range field.MapKeys() {
							merged.SetMapIndex(key, field.MapIndex(key))
						}
						for _, key := range topField.MapKeys() {
							merged.SetMapIndex(key, topField.MapIndex(key))
						}
						field.Set(merged)
					}
				}
			default:
				// basic type, just use the top value
				if field.CanSet() {
					topField := top.Field(i)
					if !topField.IsZero() {
						field.Set(topField)
					}
				}
			}
		}
	}
	return bottom, nil
}
