package main

/*
CREATE TABLE tbl_energy_price_tick (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    location_id VARCHAR(10) NOT NULL,
	currency_code VARCHAR(10) NOT NULL,
	price DECIMAL(18, 5) NOT NULL,
    timestamp DATETIME NOT NULL
);

CREATE TABLE tbl_energy_price_current (
    location_id VARCHAR(10) NOT NULL,
	currency_code VARCHAR(10) NOT NULL,
	price DECIMAL(18, 5) NOT NULL,
    last_updated DATETIME NOT NULL,
	PRIMARY KEY (location_id, currency_code)
);

CREATE TABLE tbl_mining_cost_current (
    location_id VARCHAR(10) NOT NULL,
	cost_code VARCHAR(10) NOT NULL,
	currency_code VARCHAR(10) NOT NULL,
	price DECIMAL(18, 5) NOT NULL,
    last_updated DATETIME NOT NULL,
	PRIMARY KEY (location_id, cost_code, currency_code)
);

INSERT INTO tbl_mining_cost_current (location_id, cost_code, currency_code, price, last_updated) VALUES ('QLD1', 'CAPEX', 'AUD',  1, now());
INSERT INTO tbl_mining_cost_current (location_id, cost_code, currency_code, price, last_updated) VALUES ('QLD1', 'EMPLOYEE', 'AUD',  1, now());
INSERT INTO tbl_mining_cost_current (location_id, cost_code, currency_code, price, last_updated) VALUES ('QLD1', 'OFFICE', 'AUD',  1, now());


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
	LocaionID string  `json:"location_id"`
	Currency  string  `json:"currency"`
	Price     float64 `json:"price"`
}

type DBConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

var logs *log.Logger
var db *sql.DB

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_energy_price_db [Config File]", len(args))
		fmt.Println("Example: p_energy_price_db p_energy_price_db.json")
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

	db, err = sql.Open("mysql", dsn)
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

				insertTable(msg)

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

func insertTable(msg *sarama.ConsumerMessage) {
	// JSON data
	jsonData := msg.Value

	// Parse the JSON data into an InputData struct
	var input InputData
	err := json.Unmarshal(jsonData, &input)
	if err != nil {
		logs.Fatal("Error parsing JSON:", err)
	}

	if isPriceChanged(input.LocaionID, input.Currency, input.Price) {
		// Insert the current data into the table
		insertCurrentData := "INSERT INTO tbl_energy_price_current (location_id, currency_code, price, last_updated) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE price = ?, last_updated = ?"
		_, err = db.Exec(insertCurrentData, input.LocaionID, input.Currency, input.Price, time.Now(), input.Price, time.Now())
		if err != nil {
			logs.Fatal("Error inserting data into table:", err)
		}

		//logs.Println("Data inserted successfully!")

		// Insert the data into the table if the data changes
		insertTickData := "INSERT INTO tbl_energy_price_tick (location_id, currency_code, price, timestamp) VALUES (?, ?, ?, ?)"
		_, err = db.Exec(insertTickData, input.LocaionID, input.Currency, input.Price, time.Now())
		if err != nil {
			logs.Fatal("Error inserting data into table:", err)
		}

		//logs.Println("Data inserted successfully!")
	}
}

func isPriceChanged(locationID string, currencyCode string, curr_price float64) bool {
	// Prepare the SELECT statement with placeholders for the key values
	stmt, err := db.Prepare("SELECT price, last_updated FROM tbl_energy_price_current WHERE location_id=? AND currency_code=?")
	if err != nil {
		logs.Fatal(err)
	}
	defer stmt.Close()

	// Execute the SELECT statement with the key values
	rows, err := stmt.Query(locationID, currencyCode)
	if err != nil {
		logs.Fatal(err)
	}
	defer rows.Close()

	// Check if there is any data available for the specified key values
	if !rows.Next() {
		logs.Printf("No data found for Location ID: %s, Currency Code: %s\n", locationID, currencyCode)
		return true
	}

	// Retrieve the result
	var price float64
	var lastUpdated string

	err = rows.Scan(&price, &lastUpdated)
	if err != nil {
		logs.Fatal(err)
	}

	logs.Printf("Location ID: %s, Currency Code: %s, Price: %.2f, Last Updated: %s\n", locationID, currencyCode, price, lastUpdated)

	if price != curr_price {
		return true
	}

	// Check for any errors during iteration
	err = rows.Err()
	if err != nil {
		logs.Fatal(err)
	}
	return false
}
