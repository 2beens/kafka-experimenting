package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/segmentio/kafka-go"
)

func main() {
	fmt.Println("starting producer ...")

	kafkaURL := "localhost:9092"
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
	fmt.Printf("signal [%s] received, killing everything ...", receivedSig)
	cancel()
}

func produceMessages(ctx context.Context, writer *kafka.Writer) {
	fmt.Println("start producing")

	for i := 0; ; i++ {
		// check context done
		select {
		case <-ctx.Done():
			fmt.Println("closing producer")
			return
		default:
		}

		g := newGopher()
		gopherJson, err := json.Marshal(g)
		if err != nil {
			fmt.Printf("marshal gopher: %s\n", err)
			continue
		}

		fmt.Printf("gopher: %s\n", gopherJson)

		key := fmt.Sprintf("key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: gopherJson,
			Time:  time.Now(),
		}

		err = writer.WriteMessages(ctx, msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("--> produced", key)
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
