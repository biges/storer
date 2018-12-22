package storer

import (
	"errors"
)

// ErrNotFound will be returned for any of not found errors
var ErrNotFound = errors.New("not found")

// Storer defines common data storage functions
type Storer interface {
	Find(table string, query interface{}, result interface{}, pagination *PaginationParams) error
	Create(table string, object interface{}) error
	Update(table string, query interface{}, change interface{}) error
	UpdateWithOptions(table string, query interface{}, change interface{}, options interface{}) error
	Delete(table string, query interface{}) error
	Count(table string, query interface{}) (int, error)
	Close() error
	NewPaginationParams() *PaginationParams
}

// PaginationParams should've used to pass pagination parameters to data layer
type PaginationParams struct {
	Limit  int
	SortBy string
	Page   int
}
