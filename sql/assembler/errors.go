package assembler

import "errors"

var (
	// ErrDataEmpty when the data is an empty array
	ErrDataEmpty = errors.New("must be a non-empty array")
	// ErrDataNotArray when the data is not an array
	ErrDataNotArray = errors.New("must be an array or slice")
	// ErrDataNotStruct when data items are not struct or pointer to struct
	ErrDataNotStruct = errors.New("object must be a struct or pointer to a struct")
)
