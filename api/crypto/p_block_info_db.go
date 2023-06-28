package main

/*
CREATE TABLE tbl_block_info (
    blockchain VARCHAR(10) NOT NULL,
    block_height INT NOT NULL,
    reward DECIMAL(18, 2) NOT NULL,
    difficulty DECIMAL(18, 0) NOT NULL,
    timestamp DATETIME NOT NULL,
    PRIMARY KEY (blockchain, block_height)
);

CREATE TABLE tbl_blockchain_info (
    blockchain VARCHAR(10) NOT NULL,
    subsidy DECIMAL(18, 2),
    difficulty DECIMAL(18, 0),
    last_updated DATETIME NOT NULL,
    PRIMARY KEY (blockchain)
);

*/

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
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
)

type BlockData struct {
	Op string `json:"op"`
	X  BlockX `json:"x"`
}

type BlockX struct {
	TxIndexes        []int   `json:"txIndexes"`
	NTx              int     `json:"nTx"`
	EstimatedBTCSent int     `json:"estimatedBTCSent"`
	TotalBTCSent     int     `json:"totalBTCSent"`
	Reward           int     `json:"reward"`
	Size             int     `json:"size"`
	Weight           int     `json:"weight"`
	BlockIndex       int     `json:"blockIndex"`
	PrevBlockIndex   int     `json:"prevBlockIndex"`
	Height           int     `json:"height"`
	Hash             string  `json:"hash"`
	MrklRoot         string  `json:"mrklRoot"`
	Difficulty       float64 `json:"difficulty"`
	Version          int     `json:"version"`
	Time             int     `json:"time"`
	Bits             int     `json:"bits"`
	Nonce            int     `json:"nonce"`
	FoundBy          FoundBy `json:"foundBy"`
}

type FoundBy struct {
	Description string `json:"description"`
	IP          string `json:"ip"`
	Link        string `json:"link"`
	Time        int    `json:"time"`
}

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

var logs *log.Logger
var config common.Config
var db *sql.DB
var consumer sarama.Consumer

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_block_info_db [Config File]", len(args))
		fmt.Println("Example: p_block_info_db p_block_info_db.json")
		return
	}

	// Read the JSON file
	filePath := args[1]
	fileData, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	// Parse the JSON data into a struct
	config = common.Config{}
	err = json.Unmarshal(fileData, &config)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
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
	group := "block_info"

	// Create a new consumer
	consumer, err := sarama.NewConsumerGroup([]string{config.KafkaBroker}, group, nil)
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

	// Specify the topics you want to consume from
	//topics := []string{"public.blockinfo", "public.block.difficulty", "public.block.subsidy"}
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
		case "public.blockinfo":
			insertBlockInfoTable(config.Symbol, message)
		case "public.block.difficulty":
			insertBlockchainInfoTable(message, "D")
		case "public.block.subsidy":
			insertBlockchainInfoTable(message, "S")
		default:
		}

		// Mark the message as processed
		session.MarkMessage(message, "")
	}

	return nil
}

func insertBlockInfoTable(blockchain string, msg *sarama.ConsumerMessage) {
	// JSON data
	jsonData := msg.Value

	// Parse the JSON data into an InputData struct
	var input BlockData
	err := json.Unmarshal(jsonData, &input)
	if err != nil {
		logs.Fatal("Error parsing JSON:", err)
	}

	if input.Op == "block" {
		// Insert the data into the table
		insertData := "INSERT INTO tbl_block_info (blockchain, block_height, reward, difficulty, timestamp) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE reward = ?, difficulty = ?, timestamp = ?"
		_, err = db.Exec(insertData, blockchain, input.X.Height, input.X.Reward, input.X.Difficulty, time.Now(), input.X.Reward, input.X.Difficulty, time.Now())
		if err != nil {
			logs.Fatal("Error inserting data into table:", err)
		}

		logs.Printf("Data inserted successfully!")
	}

}

func insertBlockchainInfoTable(msg *sarama.ConsumerMessage, class string) {
	// JSON data
	jsonData := msg.Value

	// Parse the JSON data into an InputData struct
	var input BlockchainData
	err := json.Unmarshal(jsonData, &input)
	if err != nil {
		logs.Fatal("Error parsing JSON:", err)
	}

	if class == "S" {
		// Insert the data into the table
		insertData := "INSERT INTO tbl_blockchain_info (blockchain, subsidy, last_updated) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE subsidy = ?, last_updated = ?"
		_, err = db.Exec(insertData, input.Symbol, input.Value, time.Now(), input.Value, time.Now())
		if err != nil {
			logs.Fatal("Error inserting data into table:", err)
		}

		logs.Printf("Data inserted successfully!")
	} else if class == "D" {
		// Insert the data into the table
		insertData := "INSERT INTO tbl_blockchain_info (blockchain, difficulty, last_updated) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE difficulty = ?, last_updated = ?"
		_, err = db.Exec(insertData, input.Symbol, input.Value, time.Now(), input.Value, time.Now())
		if err != nil {
			logs.Fatal("Error inserting data into table:", err)
		}

		logs.Printf("Data inserted successfully!")
	}
}
