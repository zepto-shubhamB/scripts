package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Record struct {
	ID              int       `bson:"id"`
	UserID          int       `bson:"user_id"`
	DialogDisplayed string    `bson:"dialog_displayed"`
	CreatedOn       time.Time `bson:"created_on"`
	UpdatedOn       time.Time `bson:"updated_on"`
}

const BatchSize = 100

func main() {
	// Create or open a log file
	logFile, err := os.OpenFile("errors.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer logFile.Close()

	// Set the logger to write to the file
	logger := log.New(logFile, "", log.LstdFlags)

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	records := []interface{}{}
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		logger.Printf("Failed to connect to MongoDB: %v", err)
		return
	}
	defer client.Disconnect(context.TODO())

	collection := client.Database("your_database_name").Collection("your_collection_name")

	// Open CSV file
	file, err := os.Open("data.csv")
	if err != nil {
		logger.Printf("Failed to open CSV file: %v", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Skip the header line
	if _, err := reader.Read(); err != nil {
		logger.Printf("Failed to read CSV header: %v", err)
		return
	}

	// Read and process each line in the CSV
	for {
		line, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			logger.Printf("Failed to read CSV line: %v", err)
			continue
		}

		// Parse CSV values
		userID, err := strconv.Atoi(line[1])
		if err != nil {
			logger.Printf("Invalid user_id: %v", err)
			continue
		}
		createdOn, err := time.Parse("2006-01-02T15:04:05Z", line[3])
		if err != nil {
			logger.Printf("Invalid created_on date: %v", err)
			continue
		}
		updatedOn, err := time.Parse("2006-01-02T15:04:05Z", line[4])
		if err != nil {
			logger.Printf("Invalid updated_on date: %v", err)
			continue
		}

		record := Record{
			UserID:          userID,
			DialogDisplayed: line[2],
			CreatedOn:       createdOn,
			UpdatedOn:       updatedOn,
		}

		err = findAndInsert(context.Background(), record, collection, &records, logger)
		if err != nil {
			logger.Printf("Failed to find and insert record: %v", err)
		}
	}

	// Insert any remaining records
	if len(records) > 0 {
		_, err := collection.InsertMany(context.Background(), records)
		if err != nil {
			logger.Printf("Failed to insert remaining records: %v", err)
		}
	}
}

func findAndInsert(ctx context.Context, record Record, collection *mongo.Collection, records *[]interface{}, logger *log.Logger) error {
	var existingRecord Record
	err := collection.FindOne(ctx, bson.M{"user_id": record.UserID}).Decode(&existingRecord)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			*records = append(*records, record)
		} else {
			return fmt.Errorf("failed to find record: %v", err)
		}
	} else if existingRecord.UpdatedOn.Before(record.CreatedOn) {
		*records = append(*records, record)
	} else {
		logger.Printf("No update needed for record with UserID: %d\n", record.UserID)
	}

	if len(*records) >= BatchSize {
		_, err := collection.InsertMany(ctx, *records)
		if err != nil {
			return fmt.Errorf("failed to insert records: %v", err)
		}
		*records = []interface{}{}
	}

	return nil
}
