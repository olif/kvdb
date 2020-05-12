package kvdb

import (
	"errors"
	"fmt"
)

var (
	notFoundError   *NotFoundError
	badRequestError *BadRequestError
)

// Store defines the kvdb public interface
type Store interface {
	// Get returns the value for the given key or any error encountered. If the
	// key was not found it will return a NotFoundError.
	Get(key string) ([]byte, error)
	// Put stores the value. It will return an BadRequestError if the provided
	// data was invalid or any other error encountered.
	Put(key string, value []byte) error
	// Delete deletes the value for the given key
	Delete(key string) error
	// Close closes the database and returns when all internal processes
	// has stopped. It returns any error encountered.
	Close() error
	// Returns true if the error signals that the given key was not found.
	IsNotFoundError(err error) bool
	// Returns true if the error signals that the consumer did something wrong.
	IsBadRequestError(err error) bool
}

// NotFoundError indicates that no value was found for the given key
type NotFoundError struct {
	missingKey string
}

// NewNotFoundError returns a new error for the missing key
func NewNotFoundError(missingKey string) error {
	return &NotFoundError{missingKey}
}

func (n *NotFoundError) Error() string {
	return fmt.Sprintf("Could not find value for key: %s", n.missingKey)
}

// NewBadRequestError returns a new BadRequestError given an error message
func NewBadRequestError(message string) error {
	return &BadRequestError{message}
}

// BadRequestError represents an error by the consumer of the database
type BadRequestError struct {
	message string
}

func (b *BadRequestError) Error() string {
	return b.message
}

// IsNotFoundError returns true if the error, or any of the wrapped errors
// is of type BadRequestError
func IsNotFoundError(err error) bool {
	return errors.As(err, &notFoundError)
}

// IsBadRequestError returns true if the error, or any of the wrapped errors
// is of type BadRequestError
func IsBadRequestError(err error) bool {
	return errors.As(err, &badRequestError)
}
