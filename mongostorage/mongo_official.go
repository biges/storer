package mongostorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/biges/mgo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"

	"github.com/biges/cyclops/storer"
)

// MongoStorage holds session and dial info of MongoDB connection
type MongoStorageOfficial struct {
	options                 *options.ClientOptions
	client                  *mongo.Client
	session                 *mongo.Database
	ctx                     context.Context
	DefaultPaginationParams *storer.PaginationParams
}

// NewMongoStorage returns a new MongoStorage with an active session
func NewMongoStorageOfficial(uri string) (*MongoStorageOfficial, error) {

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("can't connect to MongoDB: %v", err)
	}
	database := client.Database(clientOptions.Auth.AuthSource)
	return &MongoStorageOfficial{
		session: database,
		client:  client,
		options: clientOptions,
		ctx:     ctx,
		DefaultPaginationParams: &storer.PaginationParams{
			Limit:  50,
			SortBy: "_id",
			Page:   0,
		},
	}, nil
}

// Find returns all matching documents with query and pagination params
func (s *MongoStorageOfficial) Find(collectionName string, query interface{}, result interface{}, pagination *storer.PaginationParams) error {

	s.ctx, _ = context.WithTimeout(context.Background(), 30*time.Second)
	collection := s.session.Collection(collectionName)
	//filter options
	filterOptions := options.Find()
	if pagination == nil {
		pagination = s.DefaultPaginationParams
	}
	skipVal := int64(pagination.Page * pagination.Limit)
	limitVal := int64(pagination.Limit)
	filterOptions.SetSort(bson.D{{strings.Split(pagination.SortBy, ",")[0], -1}})
	filterOptions.Skip = &skipVal
	filterOptions.Limit = &limitVal

	cur, err := collection.Find(s.ctx, query, filterOptions)
	if err != nil {
		return err
	}

	var results []bson.M
	if err := cur.All(s.ctx, &results); err != nil {
		return err
	}

	if err := cur.Err(); err != nil {
		return err
	}

	jsonString, _ := bson.Marshal(results)
	err = bson.Unmarshal(jsonString, result)
	if err != nil {
		return err
	}

	return nil
}

// FindOne returns matching document
func (s *MongoStorageOfficial) FindOne(collectionName string, query interface{}, result interface{}) error {
	collection := s.session.Collection(collectionName)
	err := collection.FindOne(s.ctx, query).Decode(result)

	if err != nil {
		return err
	}

	return nil
}

// Create inserts given object to store
func (s *MongoStorageOfficial) Create(collectionName string, object interface{}) error {
	collection := s.session.Collection(collectionName)
	_, err := collection.InsertOne(s.ctx, object)

	if err != nil {
		return err
	}

	return nil
}

// Create inserts given list of object to store
func (s *MongoStorageOfficial) CreateMany(collectionName string, objects []interface{}) error {
	collection := s.session.Collection(collectionName)
	_, err := collection.InsertMany(s.ctx, objects)

	if err != nil {
		return err
	}

	return nil
}

// Update updates record with given object
func (s *MongoStorageOfficial) Update(collectionName string, query interface{}, change interface{}) error {
	collection := s.session.Collection(collectionName)
	_, err := collection.UpdateOne(s.ctx, query, change)

	if err != nil {
		return err
	}

	return nil
}

// Update updates record with given lis of object object
func (s *MongoStorageOfficial) UpdateMany(collectionName string, query interface{}, change interface{}) error {
	collection := s.session.Collection(collectionName)
	_, err := collection.UpdateMany(s.ctx, query, change)

	if err != nil {
		return err
	}

	return nil
}

// UpdateWithOptions updates record with given object  - not implemented because of official mongo driver has not method like this
func (s *MongoStorageOfficial) UpdateWithOptions(collection string, query interface{}, change interface{}, options interface{}) error {
	return errors.New("not implemented use Update or UpdateMany")
}

// Delete remove object with given id from store
func (s *MongoStorageOfficial) Delete(collectionName string, query interface{}) error {
	collection := s.session.Collection(collectionName)
	_, err := collection.DeleteOne(s.ctx, query)

	if err != nil {
		return err
	}

	return nil
}

// Delete remove object with given list of ids from store
func (s *MongoStorageOfficial) DeleteMany(collectionName string, query interface{}) error {
	collection := s.session.Collection(collectionName)
	_, err := collection.DeleteMany(s.ctx, query)

	if err != nil {
		return err
	}

	return nil
}

// Count retrieves object count directly from dbms
func (s *MongoStorageOfficial) Count(collectionName string, query interface{}) (int, error) {
	collection := s.session.Collection(collectionName)
	docCount, err := collection.CountDocuments(s.ctx, query)

	if err != nil {
		return 0, err
	}

	return int(docCount), nil
}

// Aggregate aggregate object(s) directly from dbms
func (s *MongoStorageOfficial) Aggregate(collectionName string, query interface{}, result interface{}) error {
	collection := s.session.Collection(collectionName)
	cur, err := collection.Aggregate(s.ctx, query)

	if err != nil {
		return err
	}

	var results []bson.M
	if err := cur.All(s.ctx, &results); err != nil {
		return err
	}

	if err := cur.Err(); err != nil {
		return err
	}

	jsonString, _ := json.Marshal(results)
	err = json.Unmarshal(jsonString, result)
	if err != nil {
		return err
	}

	return nil
}


//EnsureIndex is set index to mongodb - not implemented because of official mongo driver has not method like this
func (s *MongoStorageOfficial) EnsureIndex(collection string, index mgo.Index) error {
	return errors.New("not implemented")
}

// Close connection
func (s *MongoStorageOfficial) Close() error {
	return s.client.Disconnect(s.ctx)
}

// NewPaginationParams returns default pagination params
func (s *MongoStorageOfficial) NewPaginationParams() *storer.PaginationParams {
	return &storer.PaginationParams{
		SortBy: "_id",
		Page:   0,
		Limit:  50,
	}
}
