package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// set up kafka consumer config

	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "console-consumer-21564",
		"auto.offset.reset": "earliest",
	}

	// create kafka consmer

	c, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("error creating new consumer: %s", err)
	}

	defer c.Close()

	// subscribe to target topic

	c.SubscribeTopics([]string{"coordinates"}, nil)

	// consume message

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Received message %s\n", string(msg.Value))
		} else {
			fmt.Printf("Error consuming message: %v (%v)\n", err, msg)
		}

	}

}
