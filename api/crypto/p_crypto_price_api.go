package main

/*
sh /usr/local/kafka/bin/zookeeper-server-start.sh /usr/local/kafka/config/zookeeper.properties
sh /usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
sh /usr/local/kafka/bin/kafka-topics.sh --create --topic public.cryptoprice --bootstrap-server 10.0.2.15:9092
sh /usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server 10.0.2.15:9092
*/

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	common "profitmax/util/common"
	logger "profitmax/util/logger"

	"github.com/Shopify/sarama"
)

type CryptoPriceRespData struct {
	Currency string  `json:"currency"`
	Price    float64 `json:"price"`
}

type OutputData struct {
	Symbol   string  `json:"symbol"`
	Currency string  `json:"currency"`
	Price    float64 `json:"price"`
}

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_crytp_price_api [Config File]", len(args))
		fmt.Println("Example: p_crytp_price_api p_crypto_price_api.json")
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

	logs := log.New(logFile, "", log.LstdFlags)

	// Create a Kafka producer
	producer, err := sarama.NewSyncProducer([]string{config.KafkaBroker}, nil)
	if err != nil {
		logs.Fatalln("Error creating Kafka producer:", config.KafkaBroker, err)
		return
	}
	defer producer.Close()

	// Create a ticker that ticks every x seconds
	timeInterval := config.TimeInterval
	ticker := time.NewTicker(time.Duration(timeInterval) * time.Second)

	// Run the loop indefinitely
	for range ticker.C {
		// Make the HTTP GET request
		resp, err := http.Get(config.URL)
		if err != nil {
			logs.Println("Error making request:", err)
			continue
		}

		// Read the response body
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			logs.Println("Error reading response:", err)
			continue
		}
		logs.Println("[IN]: " + string(body))

		// Parse the crypto price data from the response body
		var CryptoPriceRespData map[string]float64
		err = json.Unmarshal(body, &CryptoPriceRespData)
		if err != nil {
			logs.Println("Error parsing crypto price data:", err)
			continue
		}

		// Extract the crypto symbol and price
		var currency string
		var price float64
		for s, p := range CryptoPriceRespData {
			currency = s
			price = p
		}
		// Create the CryptoPrice struct
		cryptoPrice := OutputData{
			Symbol:   config.Symbol,
			Currency: currency,
			Price:    price,
		}

		// Convert CryptoPriceRespData struct to JSON
		cryptoPriceJSON, err := json.Marshal(cryptoPrice)
		if err != nil {
			logs.Println("Error marshaling crypto price data:", err)
			continue
		}

		// Print the response
		logs.Println("[OUT]: " + string(cryptoPriceJSON))

		// Send the response to Kafka topic
		message := &sarama.ProducerMessage{
			Topic: config.Topic,
			Value: sarama.StringEncoder(cryptoPriceJSON),
		}
		_, _, err = producer.SendMessage(message)
		if err != nil {
			logs.Println("Error sending message to Kafka:", err)
			continue
		}
	}
}
