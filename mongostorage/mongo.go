package mongostorage

import (
	"fmt"
	"log"

	"github.com/ahmet/storer"
	mgo "gopkg.in/mgo.v2"
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

	session, dialErr := mgo.Dial(uri)
	if dialErr != nil {
		return nil, fmt.Errorf("can't connect to MongoDB: %v", dialErr)
	}

	session.SetSafe(&mgo.Safe{})
	log.Printf("Connected to MongoDB: %s", uri)

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
		Sort(pagination.SortBy).
		Skip(pagination.Page * pagination.Limit).
		Limit(pagination.Limit).All(result)
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

// Delete remove object with given id from store
func (s *MongoStorage) Delete(collection string, query interface{}) error {
	session := s.session.Clone()
	defer session.Close()

	_, err := session.DB(s.dialInfo.Database).C(collection).RemoveAll(query)
	return err
}

// Close connection
func (s *MongoStorage) Close() error {
	s.session.Close()
	return nil
}
