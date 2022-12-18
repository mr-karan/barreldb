package barrel

import "errors"

var (
	ErrLocked   = errors.New("a lockfile already exists")
	ErrReadOnly = errors.New("operation not allowed in read only mode")

	ErrChecksumMismatch = errors.New("invalid data: checksum does not match")

	ErrEmptyKey   = errors.New("invalid key: key cannot be empty")
	ErrExpiredKey = errors.New("invalid key: key is already expired")
	ErrLargeKey   = errors.New("invalid key: size cannot be more than 4294967296 bytes")
	ErrNoKey      = errors.New("invalid key: key is either deleted or expired or unset")

	ErrLargeValue = errors.New("invalid value: size cannot be more than 4294967296 bytes")
)
