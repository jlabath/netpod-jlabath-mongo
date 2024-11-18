package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jlabath/netpod/server/pod"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// helper types for decoding from clojure
type HexObjID struct {
	ObjectId string
}

type filterTuple bson.E

func (r *filterTuple) UnmarshalJSON(data []byte) error {
	var slice []json.RawMessage
	if err := json.Unmarshal(data, &slice); err != nil {
		return err
	}
	if len(slice) != 2 {
		return fmt.Errorf("filter tuple must have 2 values but got %d", len(slice))
	}
	var (
		key   string
		val   interface{}
		objId HexObjID
	)
	if err := json.Unmarshal(slice[0], &key); err != nil {
		return err
	}
	r.Key = key

	//val could be objective id so try that first
	//for example
	//{"ObjectId": "000000000000000000000000"}
	if err := json.Unmarshal(slice[1], &objId); err == nil {
		if oid, err := primitive.ObjectIDFromHex(objId.ObjectId); err == nil {
			//cool it's valid oid
			r.Value = oid
			return nil
		}
	}

	//else just do interface{}
	if err := json.Unmarshal(slice[1], &val); err != nil {
		return err
	}

	r.Value = val
	return nil
}

type filterSlice []filterTuple

func (r filterSlice) ToBSOND() bson.D {
	a := []filterTuple(r)
	b := make([]bson.E, len(a))
	for i, item := range a {
		b[i] = bson.E(item)
	}
	return bson.D(b)
}

//end of helper types

func checkConnection(client *mongo.Client) bool {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		log.Println(err.Error())
		return false
	}
	return true
}

func listCollections(client *mongo.Client) pod.Handler {

	return func(ctx context.Context, args []json.RawMessage) (json.RawMessage, error) {
		var dbname string

		if err := pod.DecodeArgs(args, &dbname); err != nil {
			return nil, err
		}

		// Define the database to list collections from
		database := client.Database(dbname)

		// List all collections (tables) in the database
		collections, err := database.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return nil, fmt.Errorf("trouble when ListCollectionNames: %w", err)
		}

		return json.Marshal(collections)
	}
}

func findOne(client *mongo.Client) pod.Handler {

	return func(ctx context.Context, args []json.RawMessage) (json.RawMessage, error) {
		var (
			dbname         string
			collectionName string
			filters        filterSlice
			userOptions    map[string]interface{}
		)

		//we can be called with 3 or 4 arguments
		if len(args) == 4 {
			if err := pod.DecodeArgs(args, &dbname, &collectionName, &filters, &userOptions); err != nil {
				return nil, err
			}

		} else {
			if err := pod.DecodeArgs(args, &dbname, &collectionName, &filters); err != nil {
				return nil, err
			}
		}

		// Define the database to list collections from
		database := client.Database(dbname)

		// get collection
		coll := database.Collection(collectionName)

		//populate options
		opts := options.FindOne()
		if projection, ok := userOptions["projection"]; ok {
			opts.SetProjection(projection)
		}

		var result bson.M
		err := coll.FindOne(
			ctx,
			filters.ToBSOND(),
			opts,
		).Decode(&result)
		if err != nil {
			// ErrNoDocuments means that the filter did not match any documents in
			// the collection.
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, err
			}
			return nil, fmt.Errorf("findOne failed with: %w", err)
		}

		return json.Marshal(result)
	}
}

func findMany(client *mongo.Client) pod.Handler {

	return func(ctx context.Context, args []json.RawMessage) (json.RawMessage, error) {
		var (
			dbname         string
			collectionName string
			filters        filterSlice
			userOptions    map[string]interface{}
		)

		//we can be called with 3 or 4 arguments
		if len(args) == 4 {
			if err := pod.DecodeArgs(args, &dbname, &collectionName, &filters, &userOptions); err != nil {
				return nil, err
			}

		} else {
			if err := pod.DecodeArgs(args, &dbname, &collectionName, &filters); err != nil {
				return nil, err
			}
		}

		// Define the database to list collections from
		database := client.Database(dbname)

		// get collection
		coll := database.Collection(collectionName)

		//populate options
		opts := options.Find()
		if projection, ok := userOptions["projection"]; ok {
			opts.SetProjection(projection)
		}

		if sort, ok := userOptions["sort"]; ok {
			opts.SetSort(sort)
		}

		if val, ok := userOptions["allow-disk-use"]; ok {
			if r, ok := val.(bool); ok {
				opts.SetAllowDiskUse(r)
			} else {
				log.Printf("unexpected value for allow-disk-use: %v", val)
			}
		}

		if val, ok := userOptions["limit"]; ok {
			if r, ok := val.(float64); ok {
				opts.SetLimit(int64(r))
			} else {
				log.Printf("unexpected value for limit: %v", val)
			}
		}

		var results []bson.M
		if cursor, err := coll.Find(
			ctx,
			filters.ToBSOND(),
			opts,
		); err != nil {
			return nil, fmt.Errorf("findMany failed with: %w", err)
		} else {
			if err = cursor.All(ctx, &results); err != nil {
				return nil, fmt.Errorf("findMany cursor failed with: %w", err)
			}
		}

		return json.Marshal(results)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Missing a filepath argument for socket to listen on\n")
		os.Exit(1)
	}

	// socket file path is first argument given to program
	socketPath := os.Args[1]

	//ctx for mongo client
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientOptions := options.Client().ApplyURI(os.Getenv("MONGODB_CONNECTION_URL"))
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	if checkConnection(client) {
		log.Println("Connected to MongoDB!")
	} else {
		os.Exit(1)
	}

	ds := pod.DescribeResponse{

		Format: "json",
		Namespaces: []pod.Namespace{pod.Namespace{
			Name: "netpod.jlabath.mongo",
			Vars: []pod.Var{pod.Var{
				Name:    "list-collections",
				Handler: listCollections(client)},
				pod.Var{
					Name:    "find-one",
					Handler: findOne(client),
				},
				pod.Var{
					Name:    "find-many",
					Handler: findMany(client),
				},
			}},
		}}

	os.Remove(socketPath)

	//run the server
	pod.Listen(socketPath, ds)
}
