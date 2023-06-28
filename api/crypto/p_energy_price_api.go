package main

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

type EnergyPriceData struct {
	ElecNemSummary []struct {
		SettlementDate          string  `json:"SETTLEMENTDATE"`
		RegionID                string  `json:"REGIONID"`
		Price                   float64 `json:"PRICE"`
		TotalDemand             float64 `json:"TOTALDEMAND"`
		NetInterchange          float64 `json:"NETINTERCHANGE"`
		ScheduledGeneration     float64 `json:"SCHEDULEDGENERATION"`
		SemiScheduledGeneration float64 `json:"SEMISCHEDULEDGENERATION"`
		InterconnectorFlows     string  `json:"INTERCONNECTORFLOWS"`
	} `json:"ELEC_NEM_SUMMARY"`
}

type OutputData struct {
	LocaionID string  `json:"location_id"`
	Currency  string  `json:"currency"`
	Price     float64 `json:"price"`
}

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_energy_price_api [Config File]", len(args))
		fmt.Println("Example: p_energy_price_api p_energy_price_api.json")
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
		// Send an HTTP GET request
		response, err := http.Get(config.URL)
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()

		// Read the response body
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
		}

		// Parse the JSON data
		var parsedData EnergyPriceData
		err = json.Unmarshal([]byte(body), &parsedData)
		if err != nil {
			logs.Println("Error parsing JSON:", err)
			return
		}

		// Iterate over the ElecNemSummary slice using a for loop
		for _, summary := range parsedData.ElecNemSummary {
			// Extract the desired information
			locationID := summary.RegionID
			price := summary.Price
			//logs.Println("REGIONID:", locationID)
			//logs.Println("PRICE:", price)

			// Create the EnergyPrice struct
			energyPrice := OutputData{
				LocaionID: locationID,
				Currency:  config.Currency,
				Price:     price,
			}

			// Convert EnergyPriceRespData struct to JSON
			energyPriceJSON, err := json.Marshal(energyPrice)
			if err != nil {
				logs.Fatalln("Error marshaling crypto price data:", err)
				continue
			}

			// Print the response
			logs.Println("[OUT]: " + string(energyPriceJSON))

			// Send the response to Kafka topic
			message := &sarama.ProducerMessage{
				Topic: config.Ptopic,
				Value: sarama.StringEncoder(energyPriceJSON),
			}
			_, _, err = producer.SendMessage(message)
			if err != nil {
				logs.Fatalln("Error sending message to Kafka:", err)
				continue
			}
		}
	}
}
