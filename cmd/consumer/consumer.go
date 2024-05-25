package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

const  (
	KafkaServerAddress = "localhost:9092"
	KafkaTopic = "notifications"
)

func main() {

	log.Println("Starting Consumer")

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{KafkaServerAddress}, config)
	if err != nil {
        log.Fatalf("Error creating consumer: %v", err)
    }
    defer func() {
        if err := consumer.Close(); err != nil {
            log.Fatalf("Error closing consumer: %v", err)
        }
    }()
	// Start consuming messages
    partitionConsumer, err := consumer.ConsumePartition(KafkaTopic, 0, sarama.OffsetNewest)
    if err != nil {
        log.Fatalf("Error starting partition consumer: %v", err)
    }
    defer func() {
        if err := partitionConsumer.Close(); err != nil {
            log.Fatalf("Error closing partition consumer: %v", err)
        }
    }()

    // Handle messages and errors in separate goroutines
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    go func() {
        for err := range partitionConsumer.Errors() {
            log.Printf("Error: %v", err)
        }
    }()

    go func() {
        for msg := range partitionConsumer.Messages() {
            log.Printf("Consumed message offset %d: %s = %s", msg.Offset, string(msg.Key), string(msg.Value))
        }
    }()

    // Wait for interrupt signal to exit
    <-signals
    fmt.Println("Interrupt signal received. Shutting down...")
}