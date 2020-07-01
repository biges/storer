package mongostorage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/biges/mgo"
	"github.com/biges/storer"
)

// MongoStorage holds session and dial info of MongoDB connection
type MongoStorage struct {
	session                 *mgo.Session
	dialInfo                *mgo.DialInfo
	DefaultPaginationParams *storer.PaginationParams
}

// NewMongoStorage returns a new MongoStorage with an active session
func NewMongoStorage(uri string) (*MongoStorage, error) {
	parsedURL, parseErr := mgo.ParseURL(uri)
	if parseErr != nil {
		return nil, fmt.Errorf("invalid MongoDB URI: %v", parseErr)
	}

	session, dialErr := mgo.DialWithInfo(parsedURL)
	if dialErr != nil {
		return nil, fmt.Errorf("can't connect to MongoDB: %v", dialErr)
	}

	session.SetSafe(&mgo.Safe{})

	return &MongoStorage{
		session:  session,
		dialInfo: parsedURL,
		DefaultPaginationParams: &storer.PaginationParams{
			Limit:  50,
			SortBy: "-_id",
			Page:   0,
		},
	}, nil
}

// Find returns all matching documents with query and pagination params
func (s *MongoStorage) Find(collection string, query interface{}, result interface{}, pagination *storer.PaginationParams) error {
	session := s.session.Clone()
	defer session.Close()

	if pagination == nil {
		pagination = s.DefaultPaginationParams
	}

	return session.
		DB(s.dialInfo.Database).
		C(collection).
		Find(query).
		Sort(strings.Split(pagination.SortBy, ",")...).
		Skip(pagination.Page * pagination.Limit).
		Limit(pagination.Limit).All(result)
}

// FindOne returns matching document
func (s *MongoStorage) FindOne(collection string, query interface{}, result interface{}) error {
	session := s.session.Clone()
	defer session.Close()

	return session.
		DB(s.dialInfo.Database).
		C(collection).
		Find(query).
		One(result)
}

// Create inserts given object to store
func (s *MongoStorage) Create(collection string, object interface{}) error {
	session := s.session.Clone()
	defer session.Close()

	return session.DB(s.dialInfo.Database).C(collection).Insert(object)
}

// Update updates record with given object
func (s *MongoStorage) Update(collection string, query interface{}, change interface{}) error {
	session := s.session.Clone()
	defer session.Close()

	_, err := session.DB(s.dialInfo.Database).C(collection).UpdateAll(query, change)
	return err
}

// UpdateWithOptions updates record with given object
func (s *MongoStorage) UpdateWithOptions(collection string, query interface{}, change interface{}, options interface{}) error {
	session := s.session.Clone()
	defer session.Close()

	_, err := session.DB(s.dialInfo.Database).C(collection).UpdateWithArrayFilters(query, change, options, false)
	return err
}

// Delete remove object with given id from store
func (s *MongoStorage) Delete(collection string, query interface{}) error {
	session := s.session.Clone()
	defer session.Close()

	_, err := session.DB(s.dialInfo.Database).C(collection).RemoveAll(query)
	return err
}

// Count retrieves object count directly from dbms
func (s *MongoStorage) Count(collection string, query interface{}) (int, error) {
	return s.session.
		DB(s.dialInfo.Database).
		C(collection).
		Find(query).
		Count()
}

// Aggregate aggregate object(s) directly from dbms
func (s *MongoStorage) Aggregate(collection string, query interface{}, result interface{}) error {
	return s.session.
		DB(s.dialInfo.Database).
		C(collection).
		Pipe(query).All(result)
}

// EnsureIndex is set index to mongodb
func (s *MongoStorage) EnsureIndex(collection string, index mgo.Index) error {
	return s.session.DB(s.dialInfo.Database).C(collection).EnsureIndex(index)
}

// Close connection
func (s *MongoStorage) Close() error {
	s.session.Close()
	return nil
}

func (s *MongoStorage) CreateMany(collection string, query []interface{}) error {
	return errors.New("not implemented")
}

func (s *MongoStorage) UpdateMany(collection string, query interface{}, change interface{}) error {
	return errors.New("not implemented")
}

func (s *MongoStorage) DeleteMany(collection string, query interface{}) error {
	return errors.New("not implemented")
}

// NewPaginationParams returns default pagination params
func (s *MongoStorage) NewPaginationParams() *storer.PaginationParams {
	return &storer.PaginationParams{
		SortBy: "-_id",
		Page:   0,
		Limit:  50,
	}
}
