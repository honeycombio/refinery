package collect

import (
	"fmt"
	"runtime/debug"
)

func catchPanic(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in central collector: %v\n%s", r, debug.Stack())
		}
	}()
	fn()
	return nil
}
