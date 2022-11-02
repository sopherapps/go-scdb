package scdb

import "fmt"

// ErrOutOfBounds is the error when there is an attempt to
// access or mutate a variable beyond its boundaries in memory or on disk
type ErrOutOfBounds struct {
	message string
}

func (e *ErrOutOfBounds) Error() string {
	return fmt.Sprintf("Out of bounds error: %s", e.message)
}

// NewErrOutOfBounds creates a new ErrOutOfBounds
func NewErrOutOfBounds(msg string) *ErrOutOfBounds {
	return &ErrOutOfBounds{msg}
}
