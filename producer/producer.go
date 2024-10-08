package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	// producer config
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}
	// create topic
	topic := "coordinates"

	// create kafka producer
	p, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Error creating new producer : %s", err)
	}

	// write 10 message to the "coordinates" topic
	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("message %d", i)
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic,
				Partition: kafka.PartitionAny},
			Value: []byte(value),
		}, nil)

		if err != nil {
			fmt.Printf("Failed to produce message %d: %v\n", i, err)
		} else {
			fmt.Printf("Produced message %d: %s\n", i, value)
		}
	}

	// close kafka producer
	p.Flush(15 * 1000)
	defer p.Close()
}
