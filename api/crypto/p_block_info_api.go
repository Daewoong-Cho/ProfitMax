package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	common "profitmax/util/common"
	logger "profitmax/util/logger"

	"github.com/Shopify/sarama"
	"github.com/gorilla/websocket"
)

type OutputData struct {
	MessageType string  `json:"message_type"`
	Symbol      string  `json:"symbol"`
	Value       float64 `json:"value"`
}

var logs *log.Logger
var producer sarama.SyncProducer

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_block_info_api [Config File]", len(args))
		fmt.Println("Example: p_block_info_api p_block_info_api.json")
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

	// Create a Kafka producer
	producer, err = sarama.NewSyncProducer([]string{config.KafkaBroker}, nil)
	if err != nil {
		logs.Fatalln("Error creating Kafka producer:", config.KafkaBroker, err)
		return
	}
	defer producer.Close()

	//get Difficulty/Subsidy
	ticker := time.Tick(time.Duration(config.TimeInterval) * time.Second)

	go func() {
		for range ticker {
			getValue(config.Symbol, "https://blockchain.info/q/getdifficulty", "public.block.difficulty")
			getValue(config.Symbol, "https://blockchain.info/q/bcperblock", "public.block.subsidy")
		}
	}()

	// WebSocket connection URL
	url := config.URL

	// WebSocket connection setup
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		logs.Fatal("Failed to establish WebSocket connection:", err)
	}
	defer conn.Close()

	// Send Ping message
	pingMessage := []byte(`{"op": "ping_block"}`)
	err = conn.WriteMessage(websocket.TextMessage, pingMessage)
	if err != nil {
		logs.Fatal("Failed to send Ping message:", err)
	}

	// Send Subscribe message for Unconfirmed Transactions
	subscribeMessage := []byte(`{"op": "unconfirmed_sub"}`)
	err = conn.WriteMessage(websocket.TextMessage, subscribeMessage)
	if err != nil {
		logs.Fatal("Failed to send Subscribe message for Unconfirmed Transactions:", err)
	}

	// Send Subscribe message new Block
	subscribeMessage2 := []byte(`{"op": "blocks_sub"}`)
	err = conn.WriteMessage(websocket.TextMessage, subscribeMessage2)
	if err != nil {
		logs.Fatal("Failed to send Subscribe message for new Block:", err)
	}

	// Receive messages from the WebSocket
	for {
		_, receivedMessage, err := conn.ReadMessage()
		if err != nil {
			logs.Fatal("Failed to receive message:", err)
			continue
		}

		// Process the received message
		logs.Println(string(receivedMessage))
		// Send the response to Kafka topic
		message := &sarama.ProducerMessage{
			Topic: config.Topic,
			Value: sarama.StringEncoder(receivedMessage),
		}
		_, _, err = producer.SendMessage(message)
		if err != nil {
			logs.Fatalln("Error sending message to Kafka:", err)
			continue
		}
	}

	// Send Unsubscribe message (if necessary)
	// unsubscribeMessage := []byte(`{"op": "unconfirmed_unsub"}`)
	// err = conn.WriteMessage(websocket.TextMessage, unsubscribeMessage)
	// if err != nil {
	// 	log.Fatal("Failed to send Unsubscribe message:", err)

}

func getValue(symbol string, url string, topic string) {
	resp, err := http.Get(url)
	if err != nil {
		logs.Println("API Call Error:", err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logs.Println("Error reading response:", err)
		return
	}

	value, err := strconv.ParseFloat(string(body), 64)

	// Create the OutputData struct
	OutputData := OutputData{
		Symbol: symbol,
		Value:  value,
	}

	// Convert OutputData struct to JSON
	OutputJSON, err := json.Marshal(OutputData)
	if err != nil {
		logs.Fatalln("Error marshaling blockchain difficulty data:", err)
		return
	}

	// Print the response
	logs.Println("[OUT]: " + string(OutputJSON))

	// Send the response to Kafka topic
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(OutputJSON),
	}
	_, _, err = producer.SendMessage(message)
	if err != nil {
		logs.Fatalln("Error sending message to Kafka:", err)
		return
	}
}
