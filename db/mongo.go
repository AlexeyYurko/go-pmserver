package db

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/rs/zerolog/log"

	"github.com/AlexeyYurko/go-pmserver/config"
)

// Record structure
type Record struct {
	Scraper                string  `bson:"scraper"`
	Proxy                  string  `bson:"proxy"`
	Status                 string  `bson:"status"`
	StartGetProxyTime      float64 `bson:"start_get_proxy_time"`
	NextCheck              float64 `bson:"next_check"`
	GoodAttempts           int32   `bson:"good_attempts"`
	FailedAttempts         int32   `bson:"failed_attempts"`
	DeadState              string  `bson:"dead_state"`
	LastSuccessfullyUsed   float64 `bson:"last_successfully_used"`
	NumberOfSuccessfulUses int32   `bson:"number_of_successful_uses"`
	LastFailureUsed        float64 `bson:"last_failure_used"`
	NumberOfFailures       int32   `bson:"number_of_failures"`
}

type recordsInMongo map[string]map[string]bool

// Load initial load DB from MongoDB
func Load() {
	var records []Record
	filter := bson.M{}

	client := connectToMongo()
	defer closeMongo(client)

	collection := client.Database(config.MongoDatabase).Collection(config.MongoCollection)
	cur, err := collection.Find(context.TODO(), filter)
	if err != nil {
		log.Warn().Err(err).Msg("Error on finding all the documents")
		return
	}

	if err = cur.All(context.TODO(), &records); err != nil {
		log.Warn().Err(err).Msg("Error on grabbing all the documents")
		return
	}

	counter := 0
	for recordIndex := range records {
		record := &records[recordIndex]
		scraper := record.Scraper
		currentProxy := record.Proxy

		if InProxyrack(currentProxy) {
			if config.UseProxyRack {
				Set.Store(scraper, isProxyrack, currentProxy)
			} else {
				continue
			}
		}

		status := record.Status
		if status == "" {
			status = unchecked
		}
		localStartGetProxyTime := int64(record.StartGetProxyTime)
		localNextCheck := int64(record.NextCheck)
		localLastSuccessfullyUsed := int64(record.LastSuccessfullyUsed)
		localLastFailureUsed := int64(record.LastFailureUsed)
		proxyInfo := proxy{
			Status:                 status,
			StartGetProxyTime:      localStartGetProxyTime,
			NextCheck:              localNextCheck,
			GoodAttempts:           record.GoodAttempts,
			FailedAttempts:         record.FailedAttempts,
			LastSuccessfullyUsed:   localLastSuccessfullyUsed,
			NumberOfSuccessfulUses: record.NumberOfSuccessfulUses,
			LastFailureUsed:        localLastFailureUsed,
			NumberOfFailures:       record.NumberOfFailures,
		}
		Base.Store(scraper, currentProxy, proxyInfo)
		Set.status(scraper, currentProxy, status)
		counter++
	}
	log.Info().Int("count", counter).Msg("From MongoDB loaded records")
}

func connectToMongo() (client *mongo.Client) {
	// Set client options
	clientOptions := options.Client().ApplyURI(config.MongoURI)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal().Err(err).Msg("Error on connecting to MongoDB")
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal().Err(err).Msg("Error on checking the connection to MongoDB")
	}

	log.Info().Msg("Connection to MongoDB open.")
	return
}

func closeMongo(client *mongo.Client) {
	err := client.Disconnect(context.TODO())

	if err != nil {
		log.Fatal().Err(err).Msg("Error on closing the connection to MongoDB")
	}
	log.Info().Msg("Connection to MongoDB closed.")
}

// Remove records from MongoDB
func Remove(scraper string, removedList []string) {
	var operations []mongo.WriteModel

	client := connectToMongo()
	defer closeMongo(client)
	collection := client.Database(config.MongoDatabase).Collection(config.MongoCollection)
	operationA := mongo.NewDeleteOneModel()
	for _, proxy := range removedList {
		operationA.SetFilter(bson.M{"scraper": scraper, "proxy": proxy})
		operations = append(operations, operationA)
	}
	bulkOption := options.BulkWriteOptions{}
	_, err := collection.BulkWrite(context.TODO(), operations, &bulkOption)
	if err != nil {
		log.Warn().Err(err).Msg("Error on removing records from MongoDB")
		return
	}
}

// Save to MongoDB
func Save() {
	log.Info().Msg("Save to Mongo")
	var records []Record
	var operations []mongo.WriteModel

	type update struct {
		filter  bson.M
		updates bson.M
	}
	filter := bson.M{}

	client := connectToMongo()
	defer closeMongo(client)
	collection := client.Database(config.MongoDatabase).Collection(config.MongoCollection)

	// setting cursor to find with filter
	cur, err := collection.Find(context.TODO(), filter)
	if err != nil {
		log.Warn().Err(err).Msg("Error on finding all the documents")
		return
	}

	// load all records from mongodb to var records
	if err = cur.All(context.TODO(), &records); err != nil {
		log.Warn().Err(err).Msg("Error on grabbing all the documents")
		return
	}

	if len(records) == 0 {
		// create indexes
		_, _ = collection.Indexes().CreateOne(
			context.Background(),
			mongo.IndexModel{
				Keys: bson.M{
					"proxy": 1, "scraper": 1,
				},
				Options: options.Index().SetUnique(false),
			},
		)
		_, _ = collection.Indexes().CreateOne(
			context.Background(),
			mongo.IndexModel{
				Keys: bson.M{
					"scraper": 1,
				},
				Options: options.Index().SetUnique(false),
			},
		)
	}

	savedRecords := getRecordsSavedInMongo(records)
	proxyStatuses := convertSetsToMap()
	newCounter := 0
	updateCounter := 0
	for _, scraper := range config.Scrapers {
		for proxy, record := range Base.RangeScraper(scraper) {
			if savedRecords[scraper][proxy] {
				updateCounter++
				var updateRecord update
				updateRecord.filter = bson.M{"scraper": scraper, "proxy": proxy}
				updateRecord.updates = bson.M{"$set": bson.M{
					"status":                    proxyStatuses[scraper][proxy],
					"start_get_proxy_time":      record.StartGetProxyTime,
					"next_check":                record.NextCheck,
					"good_attempts":             record.GoodAttempts,
					"failed_attempts":           record.FailedAttempts,
					"last_successfully_used":    record.LastSuccessfullyUsed,
					"number_of_successful_uses": record.NumberOfSuccessfulUses,
					"last_failure_used":         record.LastFailureUsed,
					"number_of_failures":        record.NumberOfFailures}}
				operations = append(operations, mongo.NewUpdateManyModel().SetFilter(updateRecord.filter).SetUpdate(updateRecord.updates))
			} else {
				newCounter++
				var insert = bson.M{
					"scraper":                   scraper,
					"proxy":                     proxy,
					"status":                    proxyStatuses[scraper][proxy],
					"next_check":                record.NextCheck,
					"start_get_proxy_time":      record.StartGetProxyTime,
					"good_attempts":             record.GoodAttempts,
					"failed_attempts":           record.FailedAttempts,
					"last_successfully_used":    record.LastSuccessfullyUsed,
					"number_of_successful_uses": record.NumberOfSuccessfulUses,
					"last_failure_used":         record.LastFailureUsed,
					"number_of_failures":        record.NumberOfFailures,
				}
				operations = append(operations, mongo.NewInsertOneModel().SetDocument(insert))
			}
		}
	}
	res, err := collection.BulkWrite(context.TODO(), operations)
	if err != nil {
		log.Warn().Err(err).Msg("Error on saving records to MongoDB")
		return
	}
	log.Info().Int64("inserted", res.InsertedCount).Int64("updated", res.ModifiedCount).Msg("MongoDB: insert: updated")
	log.Info().Int("insert", newCounter).Int("updated", updateCounter).Msg("Inside counters: insert: updated")
}

func getRecordsSavedInMongo(records []Record) (savedRecords recordsInMongo) {
	savedRecords = make(map[string]map[string]bool)
	for _, scraper := range config.Scrapers {
		savedRecords[scraper] = make(map[string]bool)
	}
	for recordIndex := range records {
		record := &records[recordIndex]
		recordScraper := record.Scraper
		recordProxy := record.Proxy
		savedRecords[recordScraper][recordProxy] = true
	}
	return
}

func convertSetsToMap() (proxyStatuses map[string]map[string]string) {
	toMongoStatuses := []string{good, postponed, busy, dead, unchecked}
	proxyStatuses = make(map[string]map[string]string)
	for _, scraper := range config.Scrapers {
		proxyStatuses[scraper] = make(map[string]string)
		for _, status := range toMongoStatuses {
			for _, proxy := range Set.Range(scraper, status) {
				proxyStatuses[scraper][proxy] = status
			}
		}
	}
	return
}
