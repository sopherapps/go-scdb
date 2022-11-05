package errors

import "fmt"

// ErrOutOfBounds is the error when there is an attempt to
// access or mutate a variable beyond its boundaries in memory or on disk
type ErrOutOfBounds struct {
	message string
}

func (eob *ErrOutOfBounds) Error() string {
	return fmt.Sprintf("Out of bounds error: %s", eob.message)
}

// NewErrOutOfBounds creates a new ErrOutOfBounds
func NewErrOutOfBounds(msg string) *ErrOutOfBounds {
	return &ErrOutOfBounds{msg}
}

// ErrCollisionSaturation is the error when the index slots available for a
// given hash have all been filled
type ErrCollisionSaturation struct {
	message string
}

func (ecs *ErrCollisionSaturation) Error() string {
	return fmt.Sprintf("Collision Saturation Error: %s", ecs.message)
}

// NewErrCollisionSaturation creates a new ErrCollisionSaturation
func NewErrCollisionSaturation(k []byte) *ErrCollisionSaturation {
	return &ErrCollisionSaturation{fmt.Sprintf("no free slot for %s", k)}
}
