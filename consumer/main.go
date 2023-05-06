package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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

		MaxWait:         time.Duration(1) * time.Second,
		ReadLagInterval: -1,
		//CommitInterval:  time.Duration(commitInterval),
		ErrorLogger: kafka.LoggerFunc(func(s string, i ...interface{}) {
			log.Printf(" ==> kafka error: %s", fmt.Sprintf(s, i...))
		}),
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

	// listen for interrupt signal to gracefully shut down the server
	receivedSig := <-chOsInterrupt
	log.Printf("signal [%s] received, killing everything ...", receivedSig)
	cancel()
}

func consumeMessages(ctx context.Context, reader *kafka.Reader) {
	log.Println("start consuming ...")
	for {
		// check context done
		select {
		case <-ctx.Done():
			log.Println("closing consumer")
			return
		default:
		}

		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("fetch message: %s", err)
		}
		log.Printf(
			"message at topic:%v partition:%v offset:%v	%s = %s\n",
			m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value),
		)

		//log.Println("will close reader ...")
		//if err := reader.Close(); err != nil {
		//	log.Fatalf("close reader: %s", err)
		//} else {
		//	log.Println("reader closed")
		//}

		log.Println("will commit offset ...")
		err = reader.CommitMessages(ctx, m)
		if err != nil {
			log.Fatalf("commit message: %s", err)
		}
		log.Printf("offset %d committed\n", m.Offset)

		log.Println()
	}
}
