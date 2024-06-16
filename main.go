package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
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

	// Get the MongoDB URI from environment variables
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("MONGO_URI not set in environment")
	}

	// Define the context and timeout for the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Set client options
	clientOptions := options.Client().ApplyURI(mongoURI)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	// Select the collection
	coll := client.Database("ts").Collection("temphums")

	// Calculate the start and end times for yesterday
	now := time.Now()
	yesterdayStart := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, now.Location())
	yesterdayEnd := yesterdayStart.Add(24 * time.Hour)

	// Define the aggregation pipeline
	pipeline := mongo.Pipeline{
		{{
			"$match", bson.D{
				{"updatedAt", bson.D{{"$gte", yesterdayStart}, {"$lt", yesterdayEnd}}},
			},
		}},
		{{
			"$addFields", bson.D{
				{"localHour", bson.D{
					{"$dateToString", bson.D{
						{"format", "%Y-%m-%d %H:00:00"},
						{"date", bson.D{{"$toDate", "$updatedAt"}}},
						{"timezone", "America/Chicago"},
					}},
				}},
			},
		}},
		{{
			"$group", bson.D{
				{"_id", "$localHour"},
				{"avgHumidity", bson.D{{"$avg", bson.D{{"$round", bson.A{"$humidity", 2}}}}}},
				{"avgTemperature", bson.D{{"$avg", bson.D{{"$round", bson.A{"$temperature", 2}}}}}},
			},
		}},
		{{
			"$sort", bson.D{
				{"_id", 1},
			},
		}},
	}

	// Perform the aggregation
	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	// Iterate through the cursor and print the results
	for cursor.Next(ctx) {
		var result struct {
			ID             string  `bson:"_id"`
			AvgHumidity    float64 `bson:"avgHumidity"`
			AvgTemperature float64 `bson:"avgTemperature"`
		}
		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Hour: %s, Avg Humidity: %.2f, Avg Temperature: %.2f\n", result.ID, result.AvgHumidity, result.AvgTemperature)
	}

	// Check for any errors encountered during iteration
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}
}
