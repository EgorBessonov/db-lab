package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/EgorBessonov/db-lab/lab_2/config"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"time"
)

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		logrus.Fatal("producer: %s", err)
	}
	redisClient, err := newRedisClient(cfg)
	if err != nil {
		logrus.Fatal("producer: %s", err)
	}
	defer func() {
		if err := redisClient.Close(); err != nil {
			logrus.Errorf("producer: error while closing redis client - %s", err)
		}
	}()
	c := newConsumer(redisClient, cfg.StreamName)
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*10)
	defer cancelFunc()
	exitChan := make(chan bool)
	go c.readMessages(ctx, exitChan)
	<-exitChan
}

type consumer struct {
	redisClient *redis.Client
	streamName  string
}

func newConsumer(rCli *redis.Client, streamName string) *consumer {
	return &consumer{
		redisClient: rCli,
		streamName:  streamName,
	}
}

func (c *consumer) readMessages(ctx context.Context, exitChan chan bool) {
	for {
		select {
		case <-ctx.Done():
			exitChan <- true
			return
		default:
			result, err := c.redisClient.XRead(ctx, &redis.XReadArgs{
				Streams: []string{c.streamName, "$"},
				Count:   1,
				Block:   0,
			}).Result()
			if err != nil {
				logrus.Errorf("consumer: can't read message - %s", err)
			}
			bytes := result[0].Messages[0]
			msg := bytes.Values
			msgString, ok := msg["message"].(string)
			if ok {
				var message string
				if err := json.Unmarshal([]byte(msgString), &message); err != nil {
					logrus.Errorf("consumer: can't parse message")
				}
				logrus.Println("message: ", message)
			}
		}
	}
}

func newRedisClient(cfg *config.Config) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisURL,
		Password: "",
		DB:       0,
	})
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		return nil, fmt.Errorf("can't create new redis client instance - %s", err)
	}
	return redisClient, nil
}
