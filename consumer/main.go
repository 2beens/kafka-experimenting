package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func main() {
	kafkaURL := "localhost:9092"
	topic := "gophers_part_num_1"
	groupID := "gophers_consumers_group_1"

	reader := getKafkaReader(kafkaURL, topic, groupID)
	defer reader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf(
			"message at topic:%v partition:%v offset:%v	%s = %s\n",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value),
		)
	}
}
