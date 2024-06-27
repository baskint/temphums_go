package transfer_recs

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TransferRecs() {
	// Load environment variables from .env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Load environment variables from .env.local file (overrides .env)
	err = godotenv.Overload(".env.local")
	if err != nil {
		log.Fatalf("Error loading .env.local file: %v", err)
	}

	// Get MongoDB URIs from environment variables
	sourceMongoURI := os.Getenv("SOURCE_MONGO_URI")
	if sourceMongoURI == "" {
		log.Fatal("SOURCE_MONGO_URI not set in environment")
	}
	destMongoURI := os.Getenv("DEST_MONGO_URI")
	if destMongoURI == "" {
		log.Fatal("DEST_MONGO_URI not set in environment")
	}

	// Define the context and timeout for the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect to source MongoDB
	sourceClientOptions := options.Client().ApplyURI(sourceMongoURI)
	sourceClient, err := mongo.Connect(ctx, sourceClientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := sourceClient.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// Connect to destination MongoDB
	destClientOptions := options.Client().ApplyURI(destMongoURI)
	destClient, err := mongo.Connect(ctx, destClientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := destClient.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// Select the collections
	sourceColl := sourceClient.Database("ts").Collection("temphums")
	destColl := destClient.Database("ts").Collection("temphums")

	// Define the date range for the year 2023
	startDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Find records in the year 2023
	filter := bson.D{
		{"updatedAt", bson.D{{"$gte", startDate}, {"$lt", endDate}}},
	}
	cursor, err := sourceColl.Find(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	// Prepare the records to be inserted into the destination collection
	var records []interface{}
	for cursor.Next(ctx) {
		var record bson.M
		if err := cursor.Decode(&record); err != nil {
			log.Fatal(err)
		}
		records = append(records, record)
	}

	// Insert records into the destination collection
	if len(records) > 0 {
		_, err = destColl.InsertMany(ctx, records)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Successfully transferred %d records from 2023", len(records))
	} else {
		log.Println("No records found for the year 2023")
	}
}
