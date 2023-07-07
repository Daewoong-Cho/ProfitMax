package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	common "profitmax/util/common"
	logger "profitmax/util/logger"
	"sync"

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

type InputData struct {
	LocaionID string  `json:"location_id"`
	Currency  string  `json:"currency"`
	Price     float64 `json:"price"`
}

type CurrentEnergyCost struct {
	Symbol      string  `json:"symbol"`
	Difficulty  int64   `json:"difficulty"`
	EnergyPrice float64 `json:"energy_price"`
	EnergyCost  float64 `json:"energy_cost"`
}

type CurrentCost struct {
	LocaionID  string  `json:"location_id"`
	EnergyCost float64 `json:"energy_cost"`
	OtherCost  float64 `json:"other_cost"`
	TotalCost  float64 `json:"total_cost"`
}

var logs *log.Logger
var config common.Config
var db *sql.DB
var consumer sarama.Consumer
var currentCost CurrentCost
var producer sarama.SyncProducer

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_mining_cost_calculator [Config File]", len(args))
		fmt.Println("Example: p_mining_cost_calculator p_mining_cost_calculator.json")
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
	group := "mining_cost_calculator"

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

	otherCost := getOtherCost(config.LocationID)
	energyCost := getEnergyCost(config.LocationID)

	currentCost = CurrentCost{
		config.LocationID,
		energyCost,
		otherCost,
		energyCost + otherCost,
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

func getOtherCost(location_id string) float64 {
	// Prepare the SELECT statement with placeholders for the key values
	stmt, err := db.Prepare("SELECT IFNULL(SUM(price), 0) FROM tbl_mining_cost_current WHERE cost_code <> 'ENERGY' and location_id=?")
	if err != nil {
		logs.Println(err)
		return 0
	}
	defer stmt.Close()

	// Execute the SELECT statement with the key values
	rows, err := stmt.Query(location_id)
	if err != nil {
		logs.Println(err)
		return 0
	}
	defer rows.Close()

	// Check if there is any data available for the specified key values
	if !rows.Next() {
		logs.Printf("No data found for lcoation_id: %s\n", location_id)
		return 0
	}

	// Retrieve the result
	var other_cost float64

	err = rows.Scan(&other_cost)
	if err != nil {
		logs.Println(err)
		return 0
	}

	logs.Printf("Location ID: %s, Other Cost: %.2f\n", location_id, other_cost)

	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		logs.Println(err)
		return 0
	}
	return other_cost
}

func getEnergyCost(location_id string) float64 {
	// Prepare the SELECT statement with placeholders for the key values
	stmt, err := db.Prepare("SELECT price FROM tbl_mining_cost_current WHERE cost_code = 'ENERGY' and location_id=?")
	if err != nil {
		logs.Println(err)
		return 0
	}
	defer stmt.Close()

	// Execute the SELECT statement with the key values
	rows, err := stmt.Query(location_id)
	if err != nil {
		logs.Println(err)
		return 0
	}
	defer rows.Close()

	// Check if there is any data available for the specified key values
	if !rows.Next() {
		logs.Printf("No data found for lcoation_id: %s\n", location_id)
		return 0
	}

	// Retrieve the result
	var energy_cost float64

	err = rows.Scan(&energy_cost)
	if err != nil {
		logs.Println(err)
		return 0
	}

	logs.Printf("Location ID: %s, Energy Cost: %.2f\n", location_id, energy_cost)

	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		logs.Println(err)
		return 0
	}
	return energy_cost
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
		case "private.mining.energycost":
			//
			// JSON data
			jsonData := message.Value

			// Parse the JSON data into an InputData struct
			var input CurrentEnergyCost
			err := json.Unmarshal(jsonData, &input)
			if err != nil {
				logs.Println("Error parsing JSON:", err)
				continue
			}

			// Create the OutputData struct
			currentCost.EnergyCost = input.EnergyCost
			currentCost.TotalCost = currentCost.OtherCost + input.EnergyCost

			// Convert OutputData struct to JSON
			OutputJSON, err := json.Marshal(currentCost)
			if err != nil {
				logs.Println("Error marshaling mining cost data:", err)
				continue
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
				logs.Println("Error sending message to Kafka:", err)
				continue
			}
		default:
		}

		// Mark the message as processed
		session.MarkMessage(message, "")
	}

	return nil
}
