package main

import (
	"context"
	"encoding/csv"
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

	// Create the CSV file
	csvFileName := fmt.Sprintf("measurements_%s.csv", now.Format("2006-01-02"))
	file, err := os.Create(csvFileName)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header to the CSV file
	header := []string{"measurement_date_time", "temperature_F", "humidity_percent"}
	if err := writer.Write(header); err != nil {
		log.Fatalf("Failed to write header to CSV file: %v", err)
	}

	// Iterate through the cursor and write the results to the CSV file
	for cursor.Next(ctx) {
		var result struct {
			ID             string  `bson:"_id"`
			AvgHumidity    float64 `bson:"avgHumidity"`
			AvgTemperature float64 `bson:"avgTemperature"`
		}
		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}

		// Format the record
		record := []string{
			result.ID,
			fmt.Sprintf("%.2f", result.AvgTemperature),
			fmt.Sprintf("%.2f", result.AvgHumidity),
		}

		// Write the record to the CSV file
		if err := writer.Write(record); err != nil {
			log.Fatalf("Failed to write record to CSV file: %v", err)
		}
	}

	// Check for any errors encountered during iteration
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Data successfully written to %s\n", csvFileName)
}
