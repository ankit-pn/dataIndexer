package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
	"github.com/elastic/go-elasticsearch/v7"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func indexer(mongoCollectionName string,elasticsearchIndexName string) {
	fmt.Printf(mongoCollectionName)
	fmt.Printf(elasticsearchIndexName)
	

	mongoDBURI := "mongodb://localhost:27017"
	mongoDBName := "testDb5"
	elasticsearchAddress := "https://ets.dbackup.cloud"

	// Short context for initial connections
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoDBURI))
	if err != nil {
		fmt.Printf("Error connecting to MongoDB: %v", err)
	}

	defer mongoClient.Disconnect(ctx)

	if err := mongoClient.Ping(ctx, nil); err != nil {
		fmt.Printf("Error pinging MongoDB: %v", err)
	}
	fmt.Println("Connected to MongoDB!")

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{elasticsearchAddress},
	})
	if err != nil {
		fmt.Printf("Error creating Elasticsearch client: %v", err)
	}

	_, err = esClient.Info()
	if err != nil {
		fmt.Printf("Error getting Elasticsearch info: %v", err)
	}
	fmt.Println("Connected to Elasticsearch!")

	collection := mongoClient.Database(mongoDBName).Collection(mongoCollectionName)

	// Longer context for cursor iteration
	longCtx := context.Background() // or context.WithTimeout(context.Background(), longerDuration)

	cursor, err := collection.Find(longCtx, bson.D{})
	if err != nil {
		fmt.Printf("Error finding documents: %v", err)
	}
	defer cursor.Close(longCtx)

	var bulkBuffer bytes.Buffer
	batchSize := 10000000// Adjust batch size as needed

	for cursor.Next(longCtx) {
		var document bson.M
		if err := cursor.Decode(&document); err != nil {
			fmt.Printf("Error decoding document: %v", err)
			continue
		}

		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_id":    document["_id"].(primitive.ObjectID).Hex(),
				"_index": elasticsearchIndexName,
			},
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			fmt.Printf("Error marshaling metadata: %v", err)
			continue
		}
		bulkBuffer.Write(metaBytes)
		bulkBuffer.WriteByte('\n')

		delete(document, "_id")
		docBytes, err := json.Marshal(document)
		if err != nil {
			fmt.Printf("Error marshaling document: %v", err)
			continue
		}
		bulkBuffer.Write(docBytes)
		bulkBuffer.WriteByte('\n')

		if bulkBuffer.Len() >= batchSize {
			sendBulkRequest(&bulkBuffer, esClient)
		}
	}

	if bulkBuffer.Len() > 0 {
		sendBulkRequest(&bulkBuffer, esClient)
	}
	if err := cursor.Err(); err != nil {
		fmt.Printf("Error with cursor: %v", err)
	}

	fmt.Println("Finished indexing data from MongoDB to Elasticsearch")
}

func sendBulkRequest(bulkBuffer *bytes.Buffer, esClient *elasticsearch.Client) {
	res, err := esClient.Bulk(bytes.NewReader(bulkBuffer.Bytes()))
	if err != nil {
		fmt.Printf("Failed to perform bulk request: %v", err)
		return
	}
	defer res.Body.Close()

	bulkBuffer.Reset()

	var bulkResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		fmt.Printf("Error decoding bulk response body: %v", err)
		return
	}

	if bulkResponse["errors"].(bool) {
		for _, item := range bulkResponse["items"].([]interface{}) {
			indexResponse := item.(map[string]interface{})["index"].(map[string]interface{})
			if indexResponse["error"] != nil {
				fmt.Printf("Error indexing document ID %s: %v", indexResponse["_id"], indexResponse["error"])
			}
		}
	} else {
		fmt.Println("Bulk indexing operation successful.")
	}
}
