package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/segmentio/kafka-go"
)

func main() {
	log.Println("starting producer ...")

	kafkaURL := "localhost:9093"
	topic := "gophers_part_num_1"

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	chOsInterrupt := make(chan os.Signal, 1)
	signal.Notify(chOsInterrupt, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	go produceMessages(ctx, writer)

	// listen for interrupt signal to gracefully shutdown the server
	receivedSig := <-chOsInterrupt
	log.Printf("signal [%s] received, killing everything ...", receivedSig)
	cancel()
}

func produceMessages(ctx context.Context, writer *kafka.Writer) {
	log.Println("start producing")

	for i := 0; ; i++ {
		// check context done
		select {
		case <-ctx.Done():
			log.Println("closing producer")
			return
		default:
		}

		g := newGopher()
		gopherJson, err := json.Marshal(g)
		if err != nil {
			log.Printf("marshal gopher: %s\n", err)
			continue
		}

		log.Printf("gopher: %s\n", gopherJson)

		key := fmt.Sprintf("key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: gopherJson,
			Time:  time.Now(),
		}

		err = writer.WriteMessages(ctx, msg)
		if err != nil {
			log.Println(err)
		} else {
			log.Println("--> produced", key)
		}

		time.Sleep(1 * time.Second)
	}
}

type Gopher struct {
	Age  int    `json:"age"`
	Name string `json:"name"`
}

func newGopher() Gopher {
	return Gopher{
		Age:  gofakeit.Number(1, 100),
		Name: gofakeit.Name(),
	}
}
