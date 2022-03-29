package main

import (
	"context"
	"github.com/EgorBessonov/db-lab/lab_4/config"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"strconv"
	"time"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		logrus.Fatal("producer: %s", err)
	}
	connRabbit, err := amqp.Dial(cfg.RabbitURL)
	if err != nil {
		logrus.Fatal("rabbit: connection failed - %s", err)
	}
	chanRabbit, err := connRabbit.Channel()
	if err != nil {
		logrus.Fatal("rabbit: error while creating channel - %s", err)
	}
	defer func() {
		if err := connRabbit.Close(); err != nil {
			logrus.Errorf("rabbit: error while closing connection - %s", err)
		}
		if err := chanRabbit.Close(); err != nil {
			logrus.Errorf("rabbit: error while closing rabbit chan - %s", err)
		}
	}()
	queue, err := chanRabbit.QueueDeclare(
		"TestQueue",
		true,
		false,
		false,
		false,
		nil,
	)
	rCli := NewRabbit(chanRabbit, &queue)
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute)
	defer cancelFunc()
	messages, err := rCli.Channel.Consume(
		"TestQueue", // queue name
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no local
		false,       // no wait
		nil,         // arguments
	)
	if err != nil {
		logrus.Errorf("rabbit: error while reading from queue")
	}
	go func() {
		for message := range messages {
			result, err := strconv.Atoi(string(message.Body))
			if err != nil {
				logrus.Errorf("rabbit: error while parsing message")
			}
			if result > 10 {
				logrus.Errorf("rabbit: invalid value from queue")
				continue
			}
			logrus.Println("message from queue: ", result)
		}
	}()
	<-ctx.Done()
}

// RabbitClient represent rabbitmq client structure
type RabbitClient struct {
	Channel *amqp.Channel
	Queue   *amqp.Queue
}

// NewRabbit return new RabbitClient instance
func NewRabbit(channel *amqp.Channel, queue *amqp.Queue) *RabbitClient {
	return &RabbitClient{Channel: channel, Queue: queue}
}
