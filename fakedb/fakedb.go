package fakedb

import (
	"reflect"

	"github.com/biges/storer"
)

// NewFakeStorage creates fake storage db.
func NewFakeStorage(result map[string]interface{}, err error) *FakeStorage {
	return &FakeStorage{
		result: result,
		err:    err,
		query:  make(map[string]interface{}),
		change: make(map[string]interface{}),
	}
}

// LastQuery returns last executed query
func (f FakeStorage) LastQuery(table string) interface{} {
	return f.query[table]
}

// LastChange returns last executed change query
func (f FakeStorage) LastChange(table string) interface{} {
	return f.change[table]
}

// FakeStorage represents fake database object
type FakeStorage struct {
	result map[string]interface{}
	err    error
	query  map[string]interface{}
	change map[string]interface{}
}

// Find implements fake database lookup against expected result
func (f *FakeStorage) Find(table string, query interface{}, result interface{}, pagination *storer.PaginationParams) error {
	f.query[table] = query
	resultValue := reflect.ValueOf(result)
	fResultValue := reflect.ValueOf(f.result[table])
	resultValue.Elem().Set(fResultValue)
	return f.err
}

// FindOne implements fake database lookup against expected result
func (f *FakeStorage) FindOne(table string, query interface{}, result interface{}) error {
	f.query[table] = query
	resultValue := reflect.ValueOf(result)
	fResultValue := reflect.ValueOf(f.result[table])
	resultValue.Elem().Set(fResultValue)
	return f.err
}

// Aggregate is advenced list method.
func (f *FakeStorage) Aggregate(table string, query interface{}, result interface{}) error {
	return nil
}

// Create ...
func (f *FakeStorage) Create(table string, object interface{}) error {
	return nil
}

// Update ...
func (f *FakeStorage) Update(table string, query interface{}, change interface{}) error {
	f.query[table] = query
	f.change[table] = change
	return nil
}

// UpdateWithOptions ...
func (f *FakeStorage) UpdateWithOptions(table string, query interface{}, change interface{}, options interface{}) error {
	f.query[table] = query
	f.change[table] = change
	return nil
}

// Delete just a method for satisfying storer interface.
// We are soft-deleting object... Therefore check Update method
func (f *FakeStorage) Delete(table string, query interface{}) error {
	return nil
}

// Close just a method for satisfying storer interface.
func (f *FakeStorage) Close() error {
	return nil
}

// Count should return desired object count in ideal form but it always returns zero to test out query generation.
func (f *FakeStorage) Count(table string, query interface{}) (int, error) {
	f.query[table] = query
	return 0, nil
}

// NewPaginationParams just a method for satisfying storer interface.
func (f *FakeStorage) NewPaginationParams() *storer.PaginationParams {
	return &storer.PaginationParams{}
}
