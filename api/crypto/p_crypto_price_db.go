package main

/*
CREATE TABLE tbl_crypto_price_tick (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	symbol VARCHAR(10) NOT NULL,
	price DECIMAL(18, 2) NOT NULL,
    currency_code VARCHAR(10) NOT NULL,
    timestamp DATETIME NOT NULL
);
*/

import (
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
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
)

type InputData struct {
	Symbol   string  `json:"symbol"`
	Currency string  `json:"currency"`
	Price    float64 `json:"price"`
}

type DBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

var logs *log.Logger

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_crypto_price_db [Config File]", len(args))
		fmt.Println("Example: p_crypto_price_db p_crypto_price_db.json")
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
	config := common.Config{}
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

	// Create a new consumer
	consumer, err := sarama.NewConsumer([]string{config.KafkaBroker}, conf)
	if err != nil {
		logs.Fatal("Failed to create Kafka consumer:", err)
	}
	defer consumer.Close()

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

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logs.Fatal("Error connecting to the database:", err)
	}
	defer db.Close()

	// Specify the topic and partition you want to consume from
	topic := config.Topic
	partition := int32(0)

	// Start consuming from the specified topic and partition
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		logs.Fatal("Failed to start consumer:", err)
	}

	// Create a signal channel to handle termination
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Create a wait group to wait for the consumer to finish
	wg := sync.WaitGroup{}
	wg.Add(1)

	// Start consuming messages in a separate goroutine
	go func() {
		defer wg.Done()

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				logs.Printf("Received message: Topic=%s, Partition=%d, Offset=%d, Key=%s, Value=%s\n",
					msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))

				insertTable(db, msg)

			case err := <-partitionConsumer.Errors():
				logs.Println("Error:", err.Err)

			case <-signals:
				return
			}
		}
	}()

	// Wait for a termination signal
	<-signals

	// Close the partition consumer and wait for it to finish
	partitionConsumer.Close()
	wg.Wait()
}

func insertTable(db *sql.DB, msg *sarama.ConsumerMessage) {
	// JSON data
	jsonData := msg.Value

	// Parse the JSON data into an InputData struct
	var input InputData
	err := json.Unmarshal(jsonData, &input)
	if err != nil {
		logs.Fatal("Error parsing JSON:", err)
	}

	// Insert the data into the table
	insertData := "INSERT INTO tbl_crypto_price_tick (symbol, currency_code, price, timestamp) VALUES (?, ?, ?, ?)"
	_, err = db.Exec(insertData, input.Symbol, input.Currency, input.Price, time.Now())
	if err != nil {
		logs.Fatal("Error inserting data into table:", err)
	}

	logs.Println("Data inserted successfully!")
}
