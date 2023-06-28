package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/signal"
	common "profitmax/util/common"
	logger "profitmax/util/logger"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
)

type DBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type BlockchainData struct {
	Symbol string  `json:"symbol"`
	Value  float64 `json:"value"`
}

type InputData struct {
	MessageType string  `json:"message_type"`
	Symbol      string  `json:"symbol"`
	Value       float64 `json:"value"`
}

type CurrentEnergyCost struct {
	Symbol      string  `json:"symbol"`
	Difficulty  int64   `json:"difficulty"`
	EnergyPrice float64 `json:"energy_price"`
	EnergyCost  float64 `json:"energy_cost"`
}

type InputEnergyPriceData struct {
	LocaionID string  `json:"location_id"`
	Currency  string  `json:"currency"`
	Price     float64 `json:"price"`
}

var logs *log.Logger
var config common.Config
var db *sql.DB
var consumer sarama.Consumer
var currentEnergyCost CurrentEnergyCost
var producer sarama.SyncProducer

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_energy_cost_calculator [Config File]", len(args))
		fmt.Println("Example: p_energy_cost_calculator p_energy_cost_calculator.json")
		return
	}

	// Read the JSON file
	filePath := args[1]
	fileData, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Println("Error reading file:", err)
		return
	}

	// Parse the JSON data into a struct
	config = common.Config{}
	err = json.Unmarshal(fileData, &config)
	if err != nil {
		log.Println("Error parsing JSON:", err)
		return
	}
	// Create logs directory path
	logsDir, err := logger.CreateLogsDirectory(config.LogPath)
	if err != nil {
		log.Fatal("Failed to create logs directory:", err)
	}

	// Create log file path based on the current date
	logFilePath, err := logger.CreateLogFile(logsDir, config.LogFile)
	if err != nil {
		log.Fatal("Failed to create log file path:", err)
	}

	// Open the log file
	logFile, err := os.OpenFile(logFilePath.Name(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}
	defer logFile.Close()

	logs = log.New(logFile, "", log.LstdFlags)

	// Configure the Kafka consumer
	conf := sarama.NewConfig()
	conf.Consumer.Return.Errors = true

	// Kafka consumer group
	group := "energy_cost_calculator"

	// Create a new consumer
	consumer, err := sarama.NewConsumerGroup([]string{config.KafkaBroker}, group, nil)
	if err != nil {
		logs.Fatal("Failed to create Kafka consumer:", err)
	}
	defer consumer.Close()

	// Create a Kafka producer
	producer, err = sarama.NewSyncProducer([]string{config.KafkaBroker}, nil)
	if err != nil {
		logs.Fatalln("Error creating Kafka producer:", config.KafkaBroker, err)
		return
	}
	defer producer.Close()

	// Open a connection to the MySQL database
	// Read the JSON file
	dbFilePath := "dbconfig.json"
	dbFileData, err := ioutil.ReadFile(dbFilePath)
	if err != nil {
		logs.Println("Error reading file:", err)
		return
	}
	// Parse the JSON data into a struct
	var dbConfig DBConfig
	err = json.Unmarshal(dbFileData, &dbConfig)
	if err != nil {
		logs.Println("Error parsing JSON:", err)
		return
	}

	// Create the MySQL connection string
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		logs.Fatal("Error connecting to the database:", err)
	}
	defer db.Close()

	difficulty := getDifficulty(config.Symbol)
	energyPrice := getEnergyPrice(config.LocationID)
	energyCost := calculateEnergyCost(difficulty, energyPrice)
	currentEnergyCost = CurrentEnergyCost{
		config.Symbol,
		difficulty,
		energyPrice,
		energyCost,
	}

	// Specify the topics you want to consume from
	topics := config.Topics
	// Create a context for the consumer group
	ctx := context.Background()

	// Create a signal channel to handle termination
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Create a wait group to wait for the consumer group to finish
	wg := sync.WaitGroup{}
	wg.Add(1)

	// Start consuming messages in a separate goroutine
	go func() {
		defer wg.Done()

		for {
			select {
			case <-signals:
				// Interrupt signal received, stop consuming
				consumer.Close()
				return

			default:
				// Consume messages
				err := consumer.Consume(ctx, topics, &ConsumerGroupHandler{})
				if err != nil {
					logs.Println("Error consuming messages:", err)
				}
			}
		}
	}()

	// Wait for a termination signal
	<-signals

	// Wait for the consumer group to finish
	wg.Wait()

}

// ConsumerGroupHandler implements the sarama.ConsumerGroupHandler interface
type ConsumerGroupHandler struct{}

// Setup is called when the consumer group session is being set up
func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	logs.Println("Consumer group session is being set up")
	return nil
}

// Cleanup is called when the consumer group session is ending
func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	logs.Println("Consumer group session is ending")
	return nil
}

// ConsumeClaim is called when a new set of messages is claimed by the consumer group
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		logs.Printf("Message received: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
			message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))

		switch message.Topic {
		case "public.energyprice":
			//
			// JSON data
			jsonData := message.Value

			// Parse the JSON data into an InputEnergyPriceData struct
			var input InputEnergyPriceData
			err := json.Unmarshal(jsonData, &input)
			if err != nil {
				logs.Fatal("Error parsing JSON:", err)
			}

			// Create the OutputData struct
			if config.LocationID != input.LocaionID {
				continue
			}

			if currentEnergyCost.EnergyPrice == input.Price {
				continue
			}

			currentEnergyCost.EnergyPrice = input.Price
			currentEnergyCost.EnergyCost = calculateEnergyCost(currentEnergyCost.Difficulty, currentEnergyCost.EnergyPrice)

			// Convert OutputData struct to JSON
			OutputJSON, err := json.Marshal(currentEnergyCost)
			if err != nil {
				logs.Fatalln("Error marshaling energy cost data:", err)
			}

			// Print the response
			logs.Println("[OUT]: " + string(OutputJSON))

			// Send the response to Kafka topic
			message := &sarama.ProducerMessage{
				Topic: config.Ptopic,
				Value: sarama.StringEncoder(OutputJSON),
			}
			_, _, err = producer.SendMessage(message)
			if err != nil {
				logs.Fatalln("Error sending message to Kafka:", err)
			}
		case "public.block.difficulty":
			//
			// JSON data
			jsonData := message.Value

			// Parse the JSON data into an InputData struct
			var input InputData
			err := json.Unmarshal(jsonData, &input)
			if err != nil {
				logs.Fatal("Error parsing JSON:", err)
			}

			if currentEnergyCost.Difficulty == int64(input.Value) {
				continue
			}
			// Create the OutputData struct
			currentEnergyCost.Difficulty = int64(input.Value)
			currentEnergyCost.EnergyCost = calculateEnergyCost(currentEnergyCost.Difficulty, currentEnergyCost.EnergyPrice)

			// Convert OutputData struct to JSON
			OutputJSON, err := json.Marshal(currentEnergyCost)
			if err != nil {
				logs.Fatalln("Error marshaling energy cost data:", err)
			}

			// Print the response
			logs.Println("[OUT]: " + string(OutputJSON))

			// Send the response to Kafka topic
			message := &sarama.ProducerMessage{
				Topic: config.Ptopic,
				Value: sarama.StringEncoder(OutputJSON),
			}
			_, _, err = producer.SendMessage(message)
			if err != nil {
				logs.Fatalln("Error sending message to Kafka:", err)
			}
		default:
		}

		// Mark the message as processed
		session.MarkMessage(message, "")
	}

	return nil
}

func getDifficulty(blockchain string) int64 {
	// Prepare the SELECT statement with placeholders for the key values
	stmt, err := db.Prepare("SELECT difficulty, last_updated FROM tbl_blockchain_info WHERE blockchain=?")
	if err != nil {
		logs.Fatal(err)
	}
	defer stmt.Close()

	// Execute the SELECT statement with the key values
	rows, err := stmt.Query(blockchain)
	if err != nil {
		logs.Fatal(err)
	}
	defer rows.Close()

	// Check if there is any data available for the specified key values
	if !rows.Next() {
		logs.Printf("No data found for blockchain: %s\n", blockchain)
		return 0
	}

	// Retrieve the result
	var difficulty int64
	var lastUpdated string

	err = rows.Scan(&difficulty, &lastUpdated)
	if err != nil {
		logs.Fatal(err)
	}

	logs.Printf("Blockchain: %s, Difficulty: %.2f, Last Updated: %s\n", blockchain, difficulty, lastUpdated)

	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		logs.Fatal(err)
	}
	return difficulty
}

func getEnergyPrice(LocationID string) float64 {
	// Prepare the SELECT statement with placeholders for the key values
	stmt, err := db.Prepare("SELECT price, last_updated FROM tbl_energy_price_current WHERE location_id=?")
	if err != nil {
		logs.Fatal(err)
	}
	defer stmt.Close()

	// Execute the SELECT statement with the key values
	rows, err := stmt.Query(LocationID)
	if err != nil {
		logs.Fatal(err)
	}
	defer rows.Close()

	// Check if there is any data available for the specified key values
	if !rows.Next() {
		logs.Printf("No data found for location ID: %s\n", LocationID)
		return 0
	}

	// Retrieve the result
	var energyPrice float64
	var lastUpdated string

	err = rows.Scan(&energyPrice, &lastUpdated)
	if err != nil {
		logs.Fatal(err)
	}

	logs.Printf("Location ID: %s, Energy Price: %.2f, Last Updated: %s\n", LocationID, energyPrice, lastUpdated)

	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		logs.Fatal(err)
	}
	return energyPrice
}

func calculateEnergyCost(difficulty int64, energyPrice float64) float64 {
	// Convert Bitcoin mining time (10 minutes) to seconds
	miningTime := 10 * 60

	// Calculate hash rate based on the difficulty
	hashRate := float64(difficulty) / float64(miningTime) * math.Pow(2, 32)

	// Power consumption per hash (example value, modify according to actual values)
	powerConsumptionPerHash := 0.0295454545454545 / math.Pow(10, 12) // kWh

	// Energy price per kilowatt-hour (kWh)
	energyPricePerKWh := energyPrice / 1000

	// Calculate energy cost per hash
	energyCostPerHash := powerConsumptionPerHash * energyPricePerKWh

	// Calculate hashes mined per second
	hashRatePerSecond := hashRate / float64(miningTime)

	// Calculate energy cost per second
	energyCostPerSecond := energyCostPerHash * hashRatePerSecond

	// Insert Energy Cost info to DB
	insertTable(config.LocationID, "ENERGY", config.Currency, energyCostPerSecond*float64(miningTime))
	// Convert energy cost per second to a 10-minute interval and return
	return energyCostPerSecond * float64(miningTime)
}

func insertTable(location_id string, cost_code string, currency_code string, energyCost float64) {
	// Insert the current data into the table
	insertCurrentData := "INSERT INTO tbl_mining_cost_current (location_id, cost_code, currency_code, price, last_updated) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE price = ?, last_updated = ?"
	_, err := db.Exec(insertCurrentData, location_id, cost_code, currency_code, energyCost, time.Now(), energyCost, time.Now())
	if err != nil {
		logs.Fatal("Error inserting data into table:", err)
	}
}
