package barrel

import "errors"

var (
	ErrLocked     = errors.New("a lockfile already exists")
	ErrReadOnly   = errors.New("operation not allowed in read only mode")
	ErrEmptyKey   = errors.New("empty key")
	ErrLargeKey   = errors.New("invalid key: size is too large")
	ErrLargeValue = errors.New("invalid value: size is too large")
)
