package main

import (
	"context"
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
)

type CurrentCost struct {
	LocaionID  string  `json:"location_id"`
	EnergyCost float64 `json:"energy_cost"`
	OtherCost  float64 `json:"other_cost"`
	TotalCost  float64 `json:"total_cost"`
}

type CurrentReward struct {
	Symbol string  `json:"symbol"`
	Reward float64 `json:"reward"`
}
type CurrentCrypto struct {
	Symbol   string  `json:"symbol"`
	Currency string  `json:"currency"`
	Price    float64 `json:"price"`
}

type CurrentStatus struct {
	Symbol      string  `json:"symbol"`
	Cost        float64 `json:"mining_cost"`
	Incentive   float64 `json:"mining_incentive"`
	Profits     float64 `json:"profits"`
	CryptoPrice float64 `json:"crypto_price"`
}

var logs *log.Logger
var config common.Config
var producer sarama.SyncProducer
var currentStatus CurrentStatus

func main() {
	args := os.Args

	if len(args) < 2 {
		fmt.Println("Usage: p_mining_decision_maker [Config File]", len(args))
		fmt.Println("Example: p_mining_decision_maker p_mining_decision_maker.json")
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
	group := "mining_decision_maker"

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
		case "public.cryptoprice":
			//
			// JSON data
			jsonData := message.Value

			// Parse the JSON data into an InputData struct
			var input CurrentCrypto
			err := json.Unmarshal(jsonData, &input)
			if err != nil {
				logs.Println("Error parsing JSON:", err)
				continue
			}

			// Create the OutputData struct
			currentStatus.Symbol = input.Symbol
			currentStatus.CryptoPrice = input.Price
			if currentStatus.Incentive > 0 && currentStatus.Cost > 0 {
				currentStatus.Profits = currentStatus.Incentive - currentStatus.Cost
			}
			// Convert OutputData struct to JSON
			OutputJSON, err := json.Marshal(currentStatus)
			if err != nil {
				logs.Println("Error marshaling current mining cost status data:", err)
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
		case "private.mining.cost":
			//
			// JSON data
			jsonData := message.Value

			// Parse the JSON data into an InputData struct
			var input CurrentCost
			err := json.Unmarshal(jsonData, &input)
			if err != nil {
				logs.Println("Error parsing JSON:", err)
				continue
			}

			// Create the OutputData struct
			currentStatus.Symbol = config.Symbol
			currentStatus.Cost = input.TotalCost
			if currentStatus.Incentive > 0 {
				currentStatus.Profits = currentStatus.Incentive - input.TotalCost
			}
			// Convert OutputData struct to JSON
			OutputJSON, err := json.Marshal(currentStatus)
			if err != nil {
				logs.Println("Error marshaling current mining cost status data:", err)
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
		case "private.mining.incentive":
			//
			// JSON data
			jsonData := message.Value

			// Parse the JSON data into an InputData struct
			var input CurrentReward
			err := json.Unmarshal(jsonData, &input)
			if err != nil {
				logs.Fatal("Error parsing JSON:", err)
			}

			// Create the OutputData struct
			currentStatus.Symbol = input.Symbol
			if currentStatus.CryptoPrice > 0 {
				currentStatus.Incentive = input.Reward * currentStatus.CryptoPrice
				if currentStatus.Cost > 0 {
					currentStatus.Profits = currentStatus.Incentive - currentStatus.Cost
				}
			}
			// Convert OutputData struct to JSON
			OutputJSON, err := json.Marshal(currentStatus)
			if err != nil {
				logs.Println("Error marshaling current mining incentive status data:", err)
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
