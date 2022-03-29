package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/EgorBessonov/db-lab/lab_4/config"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"math/rand"
	"time"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		logrus.Fatal("producer: %s", err)
	}
	connRabbit, err := amqp.Dial(cfg.RabbitURL)
	if err != nil {
		logrus.Fatalf("rabbit: connection failed - %s", err)
	}
	chanRabbit, err := connRabbit.Channel()
	if err != nil {
		logrus.Fatalf("rabbit: error while creating channel - %s", err)
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
	if err != nil {
		logrus.Fatal("rabbit: error while creating queue")
	}
	rCli := NewRabbit(chanRabbit, &queue)
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			value := rand.Intn(15)
			err := rCli.PublishMessage(value)
			if err != nil {
				logrus.Errorf("producer: can't publish message")
			}
		}
	}
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

// PublishMessage send message to rabbitmq queue
func (rCli *RabbitClient) PublishMessage(message int) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("rabbitmq: publishing failed - %e", err)
	}
	err = rCli.Channel.Publish("", rCli.Queue.Name, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg)})
	if err != nil {
		return fmt.Errorf("rabbitmq: publishing failed - %e", err)
	}
	return nil
}
