# Storer
Generic data store interface

## Installation
```
go get github.com/ahmet/storer
```

## Usage
```go
package main

import (
	"log"
	"os"
	"time"

	mongo "github.com/ahmet/storer/mongostorage"
	_ "github.com/joho/godotenv/autoload"
	"gopkg.in/mgo.v2/bson"
)

type server struct {
	db *mongo.MongoStorage
}

// Table Name
const table = "foo"

// foo Struct
type foo struct {
	ID        bson.ObjectId `json:"-" bson:"_id,omitempty"`
	CreatedAt time.Time     `json:"-" bson:"created_at,omitempty"`
	UpdatedAt time.Time     `json:"-" bson:"updated_at,omitempty"`
	DeletedAt time.Time     `json:"-" bson:"deleted_at,omitempty"`
	Name      string        `json:"name,omitempty" bson:"name,omitempty"`
}

// foos is a slice type for foo
type foos []foo

// List foos
func (s *server) List(query map[string]interface{}) (*foos, error) {
	result := foos{}
	err := s.db.Find(table, query, &result, nil)
	if err != nil {
		log.Printf("MongoDB failed to list : %v", err)
		return nil, err
	}

	return &result, nil
}

// Create foo
func (s *server) Create(fooElement foo) (*foo, error) {
	err := s.db.Create(table, fooElement)
	if err != nil {
		log.Printf("MongoDB failed to create : %v", err)
		return nil, err
	}
	return &fooElement, nil
}

// Update foo
func (s *server) Update(query map[string]interface{}, changeFooElement foo) (*foo, error) {
	err := s.db.Update(table, query, changeFooElement)
	if err != nil {
		log.Printf("MongoDB failed to update : %v", err)
		return nil, err
	}
	return &changeFooElement, nil
}

// Delete foo
func (s *server) Delete(query map[string]interface{}) error {
	err := s.db.Delete(table, query)
	if err != nil {
		log.Printf("MongoDB failed to delete : %v", err)
		return err
	}
	return nil
}

func main() {
	// database connection
	db, dbErr := mongo.NewMongoStorage(os.Getenv("DATABASE_URL"))
	if dbErr != nil {
		log.Fatalf("Failed to connect MongoDB: %v", dbErr)
	}
	s := server{
		db: db,
	}
}

```
