package mongostorage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/biges/storer"
	"github.com/newrelic/go-agent/v3/integrations/nrmongo"
	"github.com/newrelic/go-agent/v3/newrelic"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoStorage holds session and dial info of MongoDB connection
type MongoStorageOfficial struct {
	options                 *options.ClientOptions
	client                  *mongo.Client
	session                 *mongo.Database
	newRelicApp             *newrelic.Application
	DefaultPaginationParams *storer.PaginationParams
}

// NewMongoStorage returns a new MongoStorage with an active session
func NewMongoStorageOfficial(uri string, newRelicApp *newrelic.Application) (*MongoStorageOfficial, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nrMon := nrmongo.NewCommandMonitor(nil)
	clientOptions := options.Client().ApplyURI(uri).SetMonitor(nrMon)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("can't connect to MongoDB: %v", err)
	}

	database := client.Database("cyclops")
	return &MongoStorageOfficial{
		session:     database,
		client:      client,
		options:     clientOptions,
		newRelicApp: newRelicApp,
		DefaultPaginationParams: &storer.PaginationParams{
			Limit:  50,
			SortBy: "_id",
			Page:   0,
		},
	}, nil
}

// Find returns all matching documents with query and pagination params
func (s *MongoStorageOfficial) Find(collectionName string, query interface{}, result interface{}, pagination *storer.PaginationParams) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-find")
		ctx = newrelic.NewContext(ctx, txn)
	}

	//filter options
	filterOptions := options.Find()
	if pagination == nil {
		pagination = s.DefaultPaginationParams
	}
	skipVal := int64(pagination.Page * pagination.Limit)
	limitVal := int64(pagination.Limit)
	filterOptions.SetSort(bson.D{primitive.E{Key: strings.Split(pagination.SortBy, ",")[0], Value: -1}})
	filterOptions.Skip = &skipVal
	filterOptions.Limit = &limitVal

	collection := s.session.Collection(collectionName)
	cur, err := collection.Find(ctx, query, filterOptions)
	if err != nil {
		return err
	}

	txn.End()

	if err := cur.All(ctx, result); err != nil {
		return err
	}

	if err := cur.Err(); err != nil {
		return err
	}

	return nil
}

// FindOne returns matching document
func (s *MongoStorageOfficial) FindOne(collectionName string, query interface{}, result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-findone")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	err := collection.FindOne(ctx, query).Decode(result)
	if err != nil {
		return err
	}

	txn.End()

	return nil
}

// Create inserts given object to store
func (s *MongoStorageOfficial) Create(collectionName string, object interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-create")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	_, err := collection.InsertOne(ctx, object)
	if err != nil {
		return err
	}

	txn.End()

	return nil
}

// Create inserts given list of object to store
func (s *MongoStorageOfficial) CreateMany(collectionName string, objects []interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-createmany")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	_, err := collection.InsertMany(ctx, objects)
	if err != nil {
		return err
	}

	txn.End()

	return nil
}

// Update updates record with given object
func (s *MongoStorageOfficial) Update(collectionName string, query interface{}, change interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-update")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	_, err := collection.UpdateOne(ctx, query, change)
	if err != nil {
		return err
	}

	txn.End()

	return nil
}

// Update updates record with given lis of object object
func (s *MongoStorageOfficial) UpdateMany(collectionName string, query interface{}, change interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-updatemany")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	_, err := collection.UpdateMany(ctx, query, change)
	if err != nil {
		return err
	}

	txn.End()

	return nil
}

// UpdateWithOptions updates record with given object  - not implemented because of official mongo driver has not method like this
func (s *MongoStorageOfficial) UpdateWithOptions(collection string, query interface{}, change interface{}, options interface{}) error {
	return errors.New("not implemented use Update or UpdateMany")
}

// Delete remove object with given id from store
func (s *MongoStorageOfficial) Delete(collectionName string, query interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-delete")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	_, err := collection.DeleteOne(ctx, query)
	if err != nil {
		return err
	}

	txn.End()

	return nil
}

// Delete remove object with given list of ids from store
func (s *MongoStorageOfficial) DeleteMany(collectionName string, query interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-deletemany")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	_, err := collection.DeleteMany(ctx, query)
	if err != nil {
		return err
	}

	txn.End()

	return nil
}

// Count retrieves object count directly from dbms
func (s *MongoStorageOfficial) Count(collectionName string, query interface{}) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-count")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	docCount, err := collection.CountDocuments(ctx, query)
	if err != nil {
		return 0, err
	}

	txn.End()

	return int(docCount), nil
}

// Aggregate aggregate object(s) directly from dbms
func (s *MongoStorageOfficial) Aggregate(collectionName string, query interface{}, result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txn := new(newrelic.Transaction)
	if s.newRelicApp != nil {
		txn = s.newRelicApp.StartTransaction("mongo-aggregate")
		ctx = newrelic.NewContext(ctx, txn)
	}

	collection := s.session.Collection(collectionName)
	cur, err := collection.Aggregate(ctx, query)
	if err != nil {
		return err
	}

	txn.End()

	var results []bson.M
	if err := cur.All(ctx, &results); err != nil {
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

// Close connection
func (s *MongoStorageOfficial) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return s.client.Disconnect(ctx)
}

// NewPaginationParams returns default pagination params
func (s *MongoStorageOfficial) NewPaginationParams() *storer.PaginationParams {
	return &storer.PaginationParams{
		SortBy: "_id",
		Page:   0,
		Limit:  50,
	}
}
