package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

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

	chOsInterrupt := make(chan os.Signal, 1)
	signal.Notify(chOsInterrupt, os.Interrupt, syscall.SIGTERM)

	reader := getKafkaReader(kafkaURL, topic, groupID)
	defer reader.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go consumeMessages(ctx, reader)

	// listen for interrupt signal to gracefully shutdown the server
	receivedSig := <-chOsInterrupt
	fmt.Printf("signal [%s] received, killing everything ...", receivedSig)
	cancel()
}

func consumeMessages(ctx context.Context, reader *kafka.Reader) {
	fmt.Println("start consuming ...")
	for {
		// check context done
		select {
		case <-ctx.Done():
			fmt.Println("closing consumer")
			return
		default:
		}

		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf(
			"message at topic:%v partition:%v offset:%v	%s = %s\n",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value),
		)
	}
}
