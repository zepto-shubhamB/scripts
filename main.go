package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Record struct {
	UserID          string    `bson:"user_id"`
	DialogDisplayed string    `bson:"dialog_displayed"`
	CreatedOn       time.Time `bson:"created_on"`
	UpdatedOn       time.Time `bson:"updated_on"`
}

const BatchSize = 100

func main() {
	errLogFile, err := os.OpenFile("errors.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	successLogFile, err := os.OpenFile("success.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer errLogFile.Close()
	defer successLogFile.Close()

	errLogger := log.New(errLogFile, "", log.LstdFlags)
	successLogger := log.New(successLogFile, "", log.LstdFlags)

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017") //TODO change the URI
	records := []interface{}{}
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		errLogger.Printf("[ERROR] - Failed to connect to MongoDB: %v", err)
		return
	}
	defer client.Disconnect(context.TODO())

	collection := client.Database("your_database_name").Collection("your_collection_name")

	file, err := os.Open("data.csv")
	if err != nil {
		errLogger.Printf("[ERROR] - Failed to open CSV file: %v", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		errLogger.Printf("[ERROR] - Failed to read CSV header: %v", err)
		return
	}

	for {
		line, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			errLogger.Printf("[ERROR] - Failed to read CSV line: %v", err)
			continue
		}

		createdOn, err := time.Parse("2006-01-02T15:04:05Z", line[3])
		if err != nil {
			errLogger.Printf("[ERROR] - Invalid created_on date for userID : %s and id : %s ,err: %v", line[1], line[0], err)
			continue
		}
		updatedOn, err := time.Parse("2006-01-02T15:04:05Z", line[4])
		if err != nil {
			errLogger.Printf("[ERROR] - Invalid updated_on date for userID : %s and id : %s ,err: %v", line[1], line[0], err)
			continue
		}

		record := Record{
			UserID:          line[1],
			DialogDisplayed: line[2],
			CreatedOn:       createdOn,
			UpdatedOn:       updatedOn,
		}

		err = findAndInsert(context.Background(), record, collection, &records, errLogger, successLogger)
		if err != nil {
			for _, r := range records {
				if record, ok := r.(Record); ok {
					successLogger.Printf("[SUCCESS] - Inserted record with UserID: %s\n", record.UserID)
				} else {
					errLogger.Printf("[ERROR] - Failed to assert type for record: %v\n", r)
				}
			}
		}
	}

	if len(records) > 0 {
		_, err := collection.InsertMany(context.Background(), records)
		if err != nil {
			errLogger.Printf("[ERROR] - Failed to insert remaining records: %v", err)
		}
		for _, r := range records {
			if record, ok := r.(Record); ok {
				successLogger.Printf("[SUCCESS] - Inserted record with UserID: %s\n", record.UserID)
			} else {
				errLogger.Printf("[ERROR] - Failed to assert type for record: %v\n", r)
			}
		}
	}
}

func findAndInsert(ctx context.Context, record Record, collection *mongo.Collection, records *[]interface{}, logger *log.Logger, successLogger *log.Logger) error {
	filter := bson.M{"user_id": record.UserID}
	update := bson.M{
		"$set": bson.M{
			"dialog_displayed": record.DialogDisplayed,
			"created_on":       record.CreatedOn,
			"updated_on":       record.UpdatedOn,
		},
	}
	opts := options.Update().SetUpsert(true)

	result, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		logger.Printf("[ERROR] - failed to upsert,userID %s record: %v", record.UserID, err)
		return nil
	}

	if result.MatchedCount == 0 || result.ModifiedCount > 0 {
		successLogger.Printf("[SUCCESS] - Upserted record with UserID: %s\n", record.UserID)
	} else {
		*records = append(*records, record)
	}

	if len(*records) >= BatchSize {
		_, err := collection.InsertMany(ctx, *records)
		if err != nil {
			return fmt.Errorf("[ERROR] - failed to insert records: %v", err)
		}
		for _, r := range *records {
			if record, ok := r.(Record); ok {
				successLogger.Printf("[SUCCESS] - Inserted record with UserID: %s\n", record.UserID)
			} else {
				logger.Printf("[ERROR] - Failed to assert type for record: %v\n", r)
			}
		}
		*records = (*records)[:0]
	}

	return nil
}
